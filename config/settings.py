import os
from dotenv import load_dotenv
load_dotenv()

# Twitch API credentials
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")

# Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TWITCH_TOPIC = "twitch_streams"

# Delta Lake paths
DELTA_BRONZE_PATH = os.getenv("DELTA_BRONZE_PATH", "/delta/bronze/twitch")
DELTA_SILVER_PATH = os.getenv("DELTA_SILVER_PATH", "/delta/silver/twitch")
DELTA_GOLD_PATH = os.getenv("DELTA_GOLD_PATH", "/delta/gold/twitch")

# Streaming intervals
STREAM_INTERVAL_SECONDS = 60