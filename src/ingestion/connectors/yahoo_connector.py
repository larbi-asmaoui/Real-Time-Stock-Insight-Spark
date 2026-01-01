import logging
import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
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
        self.queue = asyncio.Queue()
        self.producer = FinanceLakeKafkaProducer()
        self.executor = ThreadPoolExecutor(max_workers=len(self.symbols))

    def fetch_sync(self, symbol):
        """
        Blocking fetch function to be run in executor.
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.fast_info
            # yfinance fast_info access might be blocking or just property access, 
            # generally safe to wrap if network calls are involved implicitly.
            return {
                "symbol": symbol,
                "price": float(info.last_price), # Corrected property name from lastPrice to last_price for fast_info
                "open": float(info.open),
                "high": float(info.day_high),
                "low": float(info.day_low),
                "volume": int(info.last_volume),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        except Exception as e:
            logging.error(f"[ERROR] Fetch {symbol}: {e}")
            return None

    async def fetch_worker(self):
        """
        Periodically fetches data for all symbols using a thread pool.
        """
        logging.info(f"Starting Yahoo async fetcher for: {self.symbols}")
        loop = asyncio.get_running_loop()
        while True:
            start_time = time.time()
            
            tasks = [
                loop.run_in_executor(self.executor, self.fetch_sync, sym)
                for sym in self.symbols
            ]
            results = await asyncio.gather(*tasks)

            for result in results:
                if result:
                    await self.queue.put(result)
            
            elapsed = time.time() - start_time
            sleep_time = max(0, self.interval - elapsed)
            await asyncio.sleep(sleep_time)

    async def send_worker(self):
        """
        Consumes from queue and sends to Kafka.
        """
        logging.info("Starting Yahoo async sender.")
        while True:
            data = await self.queue.get()
            try:
                self.producer.send(data)
            except Exception as e:
                logging.error(f"[KAFKA ERROR] {e}")
            finally:
                self.queue.task_done()

    async def run_async(self):
        await asyncio.gather(self.fetch_worker(), self.send_worker())

    def run(self):
        try:
            asyncio.run(self.run_async())
        except KeyboardInterrupt:
            logging.info("Stopping Yahoo connector.")

if __name__ == "__main__":
    YahooFinanceConnector().run()
