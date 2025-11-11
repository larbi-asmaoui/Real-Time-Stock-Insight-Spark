"""
Architecture Medallion Streaming - FIXED VERSION
Production-grade cold start handling
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode, current_timestamp, to_timestamp,
    window, count, avg, min as spark_min, max as spark_max, 
    sum as spark_sum, stddev, round as spark_round, when
)
from pyspark.sql.types import ArrayType
from processing.config import SparkConfig
from processing.schemas import StockSchemas
from processing.spark_streaming_utils import setup_logging
import time
from delta.tables import DeltaTable

logger = setup_logging()

class StockStreamingProcessor:
    """
    Processeur Medallion avec gestion cold start robuste
    Kafka → Bronze → Silver → Gold
    """
    
    def __init__(self, config=None):
        self.config = config or SparkConfig()
        self.spark = None
        self.schemas = StockSchemas()
        self.queries = {}
    
    def create_spark_session(self):
        """Crée la session Spark avec Delta Lake"""
        logger.info("🚀 Initialisation Spark Session...")
        builder = SparkSession.builder.appName(self.config.APP_NAME)
        
        for k, v in self.config.get_spark_configs().items():
            builder = builder.config(k, v)
        
        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel(self.config.LOG_LEVEL)
        logger.info(f"✅ Spark {self.spark.version} + Delta Lake activés")
        
        return self.spark
    
    def _check_delta_table_exists(self, path):
        """Vérifie si une table Delta existe et a des données"""
        try:
            if DeltaTable.isDeltaTable(self.spark, path):
                df = self.spark.read.format("delta").load(path)
                count = df.count()
                logger.info(f"✅ Table Delta trouvée: {path} ({count} records)")
                return count > 0
            else:
                logger.info(f"⚠️  Pas encore de table Delta: {path}")
                return False
        except Exception as e:
            logger.warning(f"⚠️  Table Delta non accessible: {path} - {e}")
            return False
    
    def _wait_for_data(self, path, layer_name, max_wait=120, check_interval=10):
        """Attend qu'une table Delta contienne des données"""
        logger.info(f"⏳ Attente données dans {layer_name} ({path})...")
        waited = 0
        
        while waited < max_wait:
            if self._check_delta_table_exists(path):
                logger.info(f"✅ {layer_name} prêt avec données!")
                return True
            
            time.sleep(check_interval)
            waited += check_interval
            logger.info(f"⏳ {layer_name}: {waited}/{max_wait}s écoulés...")
        
        logger.warning(f"⚠️  Timeout: {layer_name} toujours vide après {max_wait}s")
        return False
    
    # =========================================================================
    # BRONZE: Kafka → Delta Lake (données brutes)
    # =========================================================================
    
    def create_bronze_stream(self):
        """
        BRONZE: Ingestion brute depuis Kafka
        """
        logger.info("🥉 BRONZE - Démarrage ingestion Kafka...")
        
        input_schema = self.schemas.get_input_schema()
        
        # Lecture Kafka
        raw_df = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.config.KAFKA_BROKERS)
            .option("subscribe", self.config.KAFKA_TOPIC)
            .option("startingOffsets", self.config.KAFKA_STARTING_OFFSETS)
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", self.config.MAX_OFFSETS_PER_TRIGGER)
            .load()
        )
        
        # Parsing JSON
        parsed_df = (
            raw_df
            .selectExpr(
                "CAST(value AS STRING) as json_data",
                "timestamp as kafka_timestamp"
            )
            .select(
                from_json(col("json_data"), ArrayType(input_schema)).alias("data"),
                col("kafka_timestamp")
            )
            .withColumn("record", explode(col("data")))
            .select(
                "record.*",
                col("kafka_timestamp").alias("ingestion_timestamp")
            )
            .withColumn("bronze_inserted_at", current_timestamp())
        )
        
        # Écriture Bronze avec foreachBatch
        def write_bronze(batch_df, batch_id):
            if batch_df.isEmpty():
                logger.info(f"📦 Bronze batch {batch_id}: vide (aucune donnée Kafka)")
                return
                
            count = batch_df.count()
            logger.info(f"📦 Bronze batch {batch_id}: {count} records")
            
            try:
                (
                    batch_df.write
                    .format("delta")
                    .mode("append")
                    .partitionBy("symbol")
                    .option("mergeSchema", "true")
                    .save(self.config.BRONZE_PATH)
                )
                logger.info(f"✅ Bronze batch {batch_id} écrit avec succès")
            except Exception as e:
                logger.error(f"❌ Erreur Bronze batch {batch_id}: {e}")
                raise
        
        query = (
            parsed_df.writeStream
            .foreachBatch(write_bronze)
            .trigger(processingTime=self.config.PROCESSING_TIME)
            .option("checkpointLocation", f"{self.config.BASE_PATH}/checkpoints/bronze")
            .queryName("bronze_ingestion")
            .start()
        )
        
        self.queries["bronze"] = query
        logger.info(f"✅ Bronze actif → {self.config.BRONZE_PATH}")
        return query
    
    # =========================================================================
    # SILVER: Bronze → Delta Lake (enrichissement)
    # =========================================================================
    
    def create_silver_stream(self):
        """
        SILVER: Lit depuis Bronze SANS spécifier de schéma
        ⚠️ Delta Lake infère automatiquement le schéma
        """
        logger.info("🥈 SILVER - Enrichissement données...")
        
        # 🔑 FIX: PAS de .schema() avec Delta Lake!
        # Delta infère automatiquement depuis les métadonnées
        bronze_df = (
            self.spark.readStream
            .format("delta")
            # ❌ NE PAS FAIRE: .schema(bronze_schema)
            .option("ignoreChanges", "true")  # Pour les updates
            .option("ignoreDeletes", "true")  # Pour les deletes
            .load(self.config.BRONZE_PATH)
        )
        
        logger.info(f"✅ Lecture Bronze → {self.config.BRONZE_PATH}")
        
        # Transformations Silver
        silver_df = (
            bronze_df
            .withColumn("timestamp", to_timestamp(col("timestamp")))
            
            # Métriques calculées
            .withColumn("spread", col("high") - col("low"))
            .withColumn("price_change", col("price") - col("open"))
            .withColumn(
                "price_change_pct",
                when(col("open") > 0, (col("price_change") / col("open") * 100))
                .otherwise(0.0)
            )
            
            # Détection anomalies
            .withColumn(
                "is_anomaly",
                when(
                    (col("volume") == 0) |
                    (col("price") <= 0) |
                    (col("open") <= 0) |
                    (col("high") < col("low")),
                    True
                ).otherwise(False)
            )
            
            .withColumn("silver_processed_at", current_timestamp())
            .dropDuplicates(["symbol", "timestamp"])
        )
        
        # Écriture Silver
        def write_silver(batch_df, batch_id):
            if batch_df.isEmpty():
                logger.info(f"📦 Silver batch {batch_id}: vide")
                return
                
            count = batch_df.count()
            anomalies = batch_df.filter(col("is_anomaly") == True).count()
            
            logger.info(f"📦 Silver batch {batch_id}: {count} records ({anomalies} anomalies)")
            
            try:
                (
                    batch_df.write
                    .format("delta")
                    .mode("append")
                    .partitionBy("symbol")
                    .option("mergeSchema", "true")
                    .save(self.config.SILVER_PATH)
                )
                logger.info(f"✅ Silver batch {batch_id} écrit avec succès")
            except Exception as e:
                logger.error(f"❌ Erreur Silver batch {batch_id}: {e}")
                raise
        
        query = (
            silver_df.writeStream
            .foreachBatch(write_silver)
            .trigger(processingTime=self.config.PROCESSING_TIME)
            .option("checkpointLocation", f"{self.config.BASE_PATH}/checkpoints/silver")
            .queryName("silver_enrichment")
            .start()
        )
        
        self.queries["silver"] = query
        logger.info(f"✅ Silver actif → {self.config.SILVER_PATH}")
        return query
    
    # =========================================================================
    # GOLD: Silver → Delta Lake (agrégations business)
    # =========================================================================
    
    def create_gold_stream(self):
        """
        GOLD: Agrégations depuis Silver
        🔑 FIX: Pas de schéma explicite avec Delta!
        """
        logger.info("🥇 GOLD - Agrégations business...")
        
        # 🔑 FIX: Delta infère le schéma automatiquement
        silver_df = (
            self.spark.readStream
            .format("delta")
            # ❌ NE PAS FAIRE: .schema(silver_schema)
            .option("ignoreChanges", "true")
            .option("ignoreDeletes", "true")
            .load(self.config.SILVER_PATH)
        )
        
        logger.info(f"✅ Lecture Silver → {self.config.SILVER_PATH}")
        
        # Agrégations avec fenêtres
        gold_df = (
            silver_df
            .filter(col("is_anomaly") == False)
            .withWatermark("timestamp", self.config.WATERMARK_DELAY)
            
            .groupBy(
                window(col("timestamp"), self.config.WINDOW_DURATION),
                col("symbol")
            )
            .agg(
                count("*").alias("trade_count"),
                spark_round(avg("price"), 2).alias("avg_price"),
                spark_round(spark_min("price"), 2).alias("min_price"),
                spark_round(spark_max("price"), 2).alias("max_price"),
                spark_sum("volume").alias("total_volume"),
                spark_round(stddev("price"), 4).alias("volatility"),
                spark_round(avg("price_change_pct"), 2).alias("avg_price_change_pct")
            )
            
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("symbol"),
                col("avg_price"),
                col("min_price"),
                col("max_price"),
                col("volatility"),
                col("total_volume"),
                col("trade_count"),
                col("avg_price_change_pct")
            )
            .withColumn("gold_computed_at", current_timestamp())
        )
        
        # Écriture Gold
        def write_gold(batch_df, batch_id):
            if batch_df.isEmpty():
                logger.info(f"📦 Gold batch {batch_id}: vide")
                return
                
            count = batch_df.count()
            symbols = batch_df.select("symbol").distinct().count()
            
            logger.info(f"📦 Gold batch {batch_id}: {count} fenêtres, {symbols} symboles")
            
            try:
                (
                    batch_df.write
                    .format("delta")
                    .mode("append")
                    .partitionBy("symbol")
                    .option("mergeSchema", "true")
                    .save(self.config.GOLD_PATH)
                )
                logger.info(f"✅ Gold batch {batch_id} écrit avec succès")
            except Exception as e:
                logger.error(f"❌ Erreur Gold batch {batch_id}: {e}")
                raise
        
        query = (
            gold_df.writeStream
            .foreachBatch(write_gold)
            .trigger(processingTime=self.config.PROCESSING_TIME)
            .option("checkpointLocation", f"{self.config.BASE_PATH}/checkpoints/gold")
            .queryName("gold_aggregations")
            .start()
        )
        
        self.queries["gold"] = query
        logger.info(f"✅ Gold actif → {self.config.GOLD_PATH}")
        return query
    
    # =========================================================================
    # ORCHESTRATION ROBUSTE
    # =========================================================================
    
    def start_streaming(self, write_mode=None):
        """
        Lance le pipeline avec validation robuste du cold start
        """
        self.create_spark_session()
        
        logger.info("=" * 70)
        logger.info("🏗️  ARCHITECTURE MEDALLION - DÉMARRAGE")
        logger.info("=" * 70)
        logger.info("📊 Bronze: Kafka → Delta (raw)")
        logger.info("📊 Silver: Bronze → Delta (enriched)")
        logger.info("📊 Gold: Silver → Delta (aggregated)")
        logger.info("=" * 70)
        
        try:
            # ===== ÉTAPE 1: BRONZE =====
            logger.info("\n🚀 ÉTAPE 1/3: Lancement Bronze...")
            self.create_bronze_stream()
            
            # Attendre que Bronze écrive vraiment des données
            if not self._wait_for_data(
                self.config.BRONZE_PATH, 
                "Bronze", 
                max_wait=120,
                check_interval=15
            ):
                logger.warning("⚠️  Bronze démarre sans données Kafka - continuons...")
            
            # ===== ÉTAPE 2: SILVER =====
            logger.info("\n🚀 ÉTAPE 2/3: Lancement Silver...")
            self.create_silver_stream()
            
            # Attendre que Silver écrive des données
            if not self._wait_for_data(
                self.config.SILVER_PATH,
                "Silver",
                max_wait=120,
                check_interval=15
            ):
                logger.warning("⚠️  Silver démarre sans données Bronze - continuons...")
            
            # ===== ÉTAPE 3: GOLD =====
            logger.info("\n🚀 ÉTAPE 3/3: Lancement Gold...")
            self.create_gold_stream()
            
            logger.info("\n" + "=" * 70)
            logger.info("✅ PIPELINE COMPLET ACTIF!")
            logger.info("=" * 70)
            logger.info(f"🌐 Spark UI: http://localhost:4040")
            logger.info(f"📂 Bronze: {self.config.BRONZE_PATH}")
            logger.info(f"📂 Silver: {self.config.SILVER_PATH}")
            logger.info(f"📂 Gold: {self.config.GOLD_PATH}")
            logger.info(f"📂 Checkpoints: {self.config.BASE_PATH}/checkpoints/")
            logger.info("\n⏹️  Appuyez sur Ctrl+C pour arrêter...")
            logger.info("=" * 70)
            
            # Monitoring continu
            while True:
                time.sleep(30)
                self._log_streaming_status()
            
        except KeyboardInterrupt:
            logger.info("\n⏹️  Arrêt gracieux demandé...")
            self.stop_all_streams()
            logger.info("✅ Pipeline arrêté proprement")
            
        except Exception as e:
            logger.error(f"❌ Erreur critique pipeline: {e}", exc_info=True)
            self.stop_all_streams()
            raise
    
    def _log_streaming_status(self):
        """Log le statut de tous les streams"""
        logger.info("\n📊 === STATUS STREAMS ===")
        for name, query in self.queries.items():
            status = "🟢 ACTIF" if query.isActive else "🔴 ARRÊTÉ"
            recent_progress = query.recentProgress
            if recent_progress:
                last = recent_progress[-1]
                num_input = last.get("numInputRows", 0)
                logger.info(f"{status} {name.upper()}: {num_input} rows (dernier batch)")
            else:
                logger.info(f"{status} {name.upper()}: Pas encore de batch")
    
    def get_streaming_status(self):
        """Retourne le statut détaillé des streams"""
        return {
            name: {
                "id": query.id,
                "is_active": query.isActive,
                "name": query.name,
                "recent_progress": query.recentProgress
            }
            for name, query in self.queries.items()
        }
    
    def stop_all_streams(self):
        """Arrête tous les streams proprement"""
        logger.info("🛑 Arrêt de tous les streams...")
        for name, query in self.queries.items():
            try:
                if query.isActive:
                    logger.info(f"🛑 Arrêt {name}...")
                    query.stop()
                    logger.info(f"✅ {name} arrêté")
            except Exception as e:
                logger.error(f"⚠️  Erreur arrêt {name}: {e}")
        logger.info("✅ Tous les streams arrêtés")