import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

class MLConfig:
    GOLD_PATH = os.path.join(BASE_DIR, "data/lake/gold")
    FEATURES_PATH = os.path.join(BASE_DIR, "data/ml/features")
    MODEL_PATH = os.path.join(BASE_DIR, "data/ml/model")