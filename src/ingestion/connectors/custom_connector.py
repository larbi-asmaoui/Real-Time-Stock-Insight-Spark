import logging

import pandas as pd 
from ingestion.kafka_producer import FinanceLakeKafkaProducer
from stockdex import Ticker
from datetime import datetime, timezone
import time
from pytz import timezone as pytz_timezone
import os

# Configure logging
logging.basicConfig(
    filename='logs/custom_connector.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

class CustomConnector:
    def __init__(self, symbols=None, interval=10):
        self.symbols = symbols or ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA", "FB", "NFLX", "NVDA", "INTC", "AMD", "IBM", "ORCL"]
        self.interval = interval
        broker = os.getenv("KAFKA_BROKER", "kafka:29092")
        self.producer = FinanceLakeKafkaProducer(
            # broker="kafka:9092", topic="stock_prices"
            broker= broker, topic="stock_prices"
        )

    def fetch_symbol_data(self, symbol):
        try:
            ticker = Ticker(ticker=symbol)
            info = ticker.yahoo_api_price(range='1y', dataGranularity='1d')
            records = []
            for _, row in info.iterrows():
                record = {
                    "symbol": symbol,
                    "price": round(float(row["close"]), 2),
                    "open": round(float(row["open"]), 2),
                    "high": round(float(row["high"]), 2),
                    "low": round(float(row["low"]), 2),
                    "volume": int(row["volume"]),
                    "timestamp": str(row["timestamp"]),
                }
                records.append(record)
            
            return records
        except Exception as e:
            logging.error(f"Fetch {symbol}: {e}")
            return None

    def run(self):
        logging.info(f"Starting Custom connector for: {self.symbols}")
        while True:
            for sym in self.symbols:
                data = self.fetch_symbol_data(sym)
                if data:
                    self.producer.send(data)
            time.sleep(self.interval)


if __name__ == "__main__":
    CustomConnector().run()
