import logging
from pathlib import Path

def setup_ml_logging():

    project_root = Path(__file__).resolve().parents[2]
    log_dir = project_root / "logs"
    log_file = log_dir / "ml_predictor.log"

    log_dir.mkdir(parents=True, exist_ok=True)
    log_file.touch(exist_ok=True)

    logger = logging.getLogger("StockPredictor")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger