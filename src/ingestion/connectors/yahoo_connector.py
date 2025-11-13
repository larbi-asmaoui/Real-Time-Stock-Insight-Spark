import logging
from ingestion.kafka_producer import FinanceLakeKafkaProducer
import yfinance as yf
from datetime import datetime, timezone
import time



# Configure logging
logging.basicConfig(
    filename='logs/yahoo_finance_connector.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    filemode='w'
)

class YahooFinanceConnector:
    def __init__(self, symbols=None, interval=5):
        self.symbols = symbols or ["AAPL", "GOOG", "AMZN", "MSFT", "TSLA", "FB", "NFLX"]
        self.interval = interval
        self.producer = FinanceLakeKafkaProducer(
        )

    def fetch_symbol_data(self, symbol):
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.fast_info
            logging.info(f"[FETCHED] {symbol} data: {info}")
            return {
                "symbol": symbol,
                "price": float(info.get("lastPrice", 0.0)),
                "open": float(info.get("open", 0.0)),
                "high": float(info.get("dayHigh", 0.0)),
                "low": float(info.get("dayLow", 0.0)),
                "volume": int(info.get("lastVolume", 0)),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as e:
            logging.error(f"[ERROR] Fetch {symbol}: {e}")
            return None

    def run(self):
        logging.info(f"Starting Yahoo connector for: {self.symbols}")
        while True:
            for sym in self.symbols:
                data = self.fetch_symbol_data(sym)
                if data:
                    self.producer.send(data)
                time.sleep(self.interval)


if __name__ == "__main__":
    YahooFinanceConnector().run()
