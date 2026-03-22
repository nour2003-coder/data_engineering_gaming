import requests
import time
import json
from datetime import datetime
from config.settings import (
    TWITCH_CLIENT_ID, 
    TWITCH_CLIENT_SECRET, 
    STREAM_INTERVAL_SECONDS, 
    TWITCH_TOPIC
)
from kafka import KafkaProducer

# Kafka producer setup
def get_producer():
    return KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

# Get OAuth token
def get_oauth_token():
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": TWITCH_CLIENT_ID,
        "client_secret": TWITCH_CLIENT_SECRET,
        "grant_type": "client_credentials"
    }
    resp = requests.post(url, params=params).json()
    token = resp['access_token']
    expires_in = resp.get('expires_in', 3600)
    print(f"✅ Got OAuth token (expires in {expires_in}s)", flush=True)
    return token, time.time() + expires_in - 60

# Fetch live streams
def fetch_live_streams(oauth_token, first=100):
    url = "https://api.twitch.tv/helix/streams"
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {oauth_token}"
    }
    params = {"first": first}
    resp = requests.get(url, headers=headers, params=params).json()
    return resp.get('data', [])

# 🔥 NEW: Get game names from game_ids
def fetch_game_names(oauth_token, game_ids):
    if not game_ids:
        return {}

    url = "https://api.twitch.tv/helix/games"
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {oauth_token}"
    }

    params = [("id", gid) for gid in list(game_ids)[:100]]

    resp = requests.get(url, headers=headers, params=params).json()
    data = resp.get("data", [])

    return {game["id"]: game["name"] for game in data}

# 🔥 Transform stream into clean structured event
def transform_stream(stream, game_name):
    return {
        "platform": "twitch",
        "event_time": stream.get("started_at"),
        "ingestion_time": datetime.utcnow().isoformat(),

        "stream_id": stream.get("id"),
        "streamer_id": stream.get("user_id"),
        "streamer_name": stream.get("user_name"),

        "game_id": stream.get("game_id"),
        "game_name": game_name,

        "title": stream.get("title"),
        "language": stream.get("language"),

        "viewer_count": stream.get("viewer_count"),

        "is_mature": stream.get("is_mature"),
        "thumbnail_url": stream.get("thumbnail_url")
    }

# Push to Kafka
def push_streams_to_kafka(producer, streams, game_map):
    enriched_data = []

    for stream in streams:
        game_id = stream.get("game_id")
        game_name = game_map.get(game_id, "Unknown")

        transformed = transform_stream(stream, game_name)
        enriched_data.append(transformed)

        producer.send(TWITCH_TOPIC, transformed)

    producer.flush()
    print(f"🚀 Sent {len(enriched_data)} enriched streams to Kafka", flush=True)

# MAIN
if __name__ == "__main__":

    # Wait for Kafka
    while True:
        try:
            producer = get_producer()
            print("✅ Connected to Kafka!", flush=True)
            break
        except Exception as e:
            print(f"Kafka not ready yet, retrying in 5s: {e}", flush=True)
            time.sleep(5)

    # Get token
    token, token_expiry = get_oauth_token()

    while True:
        if time.time() >= token_expiry:
            token, token_expiry = get_oauth_token()

        streams = fetch_live_streams(token)

        # 🔥 Extract unique game_ids
        game_ids = set([s.get("game_id") for s in streams if s.get("game_id")])

        # 🔥 Fetch game names
        game_map = fetch_game_names(token, game_ids)

        # 🔥 Send enriched data
        push_streams_to_kafka(producer, streams, game_map)

        time.sleep(STREAM_INTERVAL_SECONDS)