from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType
from processing.config import SparkConfig
from processing.schemas import StockSchemas
from processing.spark_streaming_utils import setup_logging
import time
from delta.tables import DeltaTable

from processing.delta_lake.delta_lake_bronze import BronzeLayer
from processing.delta_lake.delta_lake_silver import SilverLayer
from processing.delta_lake.delta_lake_gold import GoldLayer

logger = setup_logging()

class StockStreamingProcessor:
    
    def __init__(self, config=None):
        self.config = config or SparkConfig()
        self.spark = None
        self.schemas = StockSchemas()
        self.queries = {}

        self.bronze_layer = None
        self.silver_layer = None
        self.gold_layer = None
    
    def create_spark_session(self):
        logger.info("🚀 Initialisation Spark Session...")
        builder = SparkSession.builder.appName(self.config.APP_NAME)
        
        for k, v in self.config.get_spark_configs().items():
            builder = builder.config(k, v)
        
        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel(self.config.LOG_LEVEL)
        logger.info(f"✅ Spark {self.spark.version} + Delta Lake activés")

        self.bronze_layer = BronzeLayer(self.spark, self.config, self.schemas)
        self.silver_layer = SilverLayer(self.spark, self.config, self.schemas)
        self.gold_layer = GoldLayer(self.spark, self.config, self.schemas)
        
        return self.spark
    
    def _check_delta_table_exists(self, path):
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
    
    def create_bronze_stream(self):
        query = self.bronze_layer.create_stream()
        self.queries["bronze"] = query
        return query
    
    def create_silver_stream(self):
        query = self.silver_layer.create_stream()
        self.queries["silver"] = query
        return query
    
    def create_gold_stream(self):
        query = self.gold_layer.create_stream()
        self.queries["gold"] = query
        return query
 
    def start_streaming(self, write_mode=None):

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