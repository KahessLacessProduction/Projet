"""Configuration settings for the Data Pipeline.
Supports local development, CI environments, and containerized deployment.
"""
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

# Data directories
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
TEXT_SAMPLES_DIR = DATA_DIR / "text_samples"

# Database Configuration (SQLite by default, PostgreSQL compatible)
DEFAULT_DB_PATH = DATA_DIR / "finance.db"
DB_URL = os.getenv("DB_URL", f"sqlite:///{DEFAULT_DB_PATH}")

# Business rules
HIGH_AMOUNT_THRESHOLD = float(os.getenv("HIGH_AMOUNT_THRESHOLD", 5000.0))

# Retry parameters
RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", 3))
RETRY_INITIAL_DELAY = float(os.getenv("RETRY_INITIAL_DELAY", 0.5))
RETRY_BACKOFF_FACTOR = float(os.getenv("RETRY_BACKOFF_FACTOR", 2.0))

# S3 / MinIO Configuration
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "finance-transactions")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

# Scheduler parameters
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", 10))
