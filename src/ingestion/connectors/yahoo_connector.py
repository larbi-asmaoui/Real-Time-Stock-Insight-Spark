from ingestion.kafka_producer import FinanceLakeKafkaProducer
import yfinance as yf
from datetime import datetime, timezone
import time


class YahooFinanceConnector:
    def __init__(self, symbols=None, interval=10):
        self.symbols = symbols or ["AAPL", "GOOG", "MSFT", "AMZN"]
        self.interval = interval
        self.producer = FinanceLakeKafkaProducer(
            broker="kafka:9092", topic="stock_prices"
        )

    def fetch_symbol_data(self, symbol):
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.fast_info
            # print(f"[FETCHED] {symbol} data: {info}")
            return {
                "symbol": symbol,
                "price": float(info.get("lastPrice", 0.0)),
                "open": float(info.get("open", 0.0)),
                "high": float(info.get("dayHigh", 0.0)),
                "low": float(info.get("dayLow", 0.0)),
                "volume": int(info.get("lastVolume", 0)),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "yahoo"
            }
        except Exception as e:
            print(f"[ERROR] Fetch {symbol}: {e}")
            return None

    def run(self):
        print(f"TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT-- Starting Yahoo connector for: {self.symbols}")
        while True:
            for sym in self.symbols:
                data = self.fetch_symbol_data(sym)
                if data:
                    self.producer.send(data)
                    print(f"[PUSHED] {data['symbol']} → {data['price']} ({data['source']})")

            time.sleep(self.interval)


if __name__ == "__main__":
    YahooFinanceConnector().run()
