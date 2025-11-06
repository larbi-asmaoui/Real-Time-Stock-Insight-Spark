from processing.spark_streaming_processor import StockStreamingProcessor
from processing.spark_streaming_utils import setup_logging

logger = setup_logging()

def main():
    """Point d'entrée principal"""
    processor = StockStreamingProcessor()
    processor.start_streaming(write_mode="console")

if __name__ == "__main__":
    main()