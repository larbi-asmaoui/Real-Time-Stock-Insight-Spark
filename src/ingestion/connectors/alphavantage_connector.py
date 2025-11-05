"""
Alpha Vantage Connector for FinanceLake
---------------------------------------
Fetches stock market data using the Alpha Vantage API and sends it to Kafka.
"""

import os
import json
import time
from datetime import datetime
import requests
from ingestion.kafka_producer import FinanceLakeKafkaProducer
import logging 


# Configure logging
logging.basicConfig(
    filename='logs/alphavantage_connector.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

class AlphaVantageConnector:
    """
    Collects stock price data using the Alpha Vantage API and publishes it to Kafka.
    """

    def __init__(self, api_key="7UYREEDLILBZ2V93",
                 symbols=None, interval="1min", fetch_delay=60):
        """
        :param api_key: Alpha Vantage API key
        :param broker: Kafka broker address
        :param topic: Kafka topic to publish to
        :param symbols: List of stock tickers
        :param interval: Interval between data points ('1min', '5min', '15min', etc.)
        :param fetch_delay: Delay between two API calls (Alpha Vantage free = 5 calls/min)
        """
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"
        self.symbols = symbols or ["AAPL", "GOOG", "MSFT", "AMZN"]
        self.interval = interval
        self.fetch_delay = fetch_delay

        # Kafka producer
        self.producer = FinanceLakeKafkaProducer(
            
            topic="stock_prices"
        )

        if not self.api_key:
            raise ValueError("Missing Alpha Vantage API key.")

    def fetch_symbol_data(self, symbol):
        """
        Fetch the most recent intraday data for a symbol.
        """
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": self.interval,
            "apikey": self.api_key,
        }

        try:
            response = requests.get(self.base_url, params=params, timeout=15)
            data = response.json()

            # Alpha Vantage returns nested dicts
            time_series = data.get(f"Time Series ({self.interval})", {})
            if not time_series:
                print(f"[WARN] No data returned for {symbol}.")
                return None

            # Get the latest timestamp
            latest_ts = sorted(time_series.keys())[-1]
            values = time_series[latest_ts]
            # logging.info(f"[FETCHED] {symbol} data at {latest_ts}: {values}")

            record = {
                "symbol": symbol,
                "price": float(values["4. close"]),
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "volume": int(values["5. volume"]),
                "timestamp": datetime.strptime(latest_ts, "%Y-%m-%d %H:%M:%S").isoformat(),
                "source": "alpha_vantage",
            }

            return record

        except Exception as e:
            print(f"[ERROR] Fetching {symbol} failed: {e}")
            return None

    def run(self):
        """
        Continuously fetches and sends data to Kafka.
        """
        logging.info(f"Alpha Vantage connector started for {self.symbols} (interval={self.interval})")
        while True:
            for symbol in self.symbols:
                data = self.fetch_symbol_data(symbol)
                if data:
                    self.producer.send(data)
                    
                    # logging.info(f"from --- [PUSHED] {data['symbol']} → {data['price']} at {data['timestamp']}")
                    time.sleep(self.fetch_delay)  # Respect API limit
            logging.info("[SLEEP] Cycle complete. Waiting before next round...")
            time.sleep(self.fetch_delay)
            

if __name__ == "__main__":
    connector = AlphaVantageConnector(
        api_key=os.getenv("ALPHA_VANTAGE_KEY", "YOUR_API_KEY"),
        symbols=["TSLA", "NFLX", "FB"],
        interval="1min",
        fetch_delay=10, 
    )
    connector.run()
