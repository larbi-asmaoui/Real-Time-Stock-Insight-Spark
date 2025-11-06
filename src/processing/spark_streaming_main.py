# spark_streaming_main.py
from .spark_streaming_processor import StockStreamingProcessor
from .spark_streaming_utils import setup_logging

logger = setup_logging()

def main():
    """Point d'entrée principal"""
    processor = StockStreamingProcessor()
    processor.start_streaming(write_mode="both")

if __name__ == "__main__":
    main()