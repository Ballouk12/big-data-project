#!/usr/bin/env python3
"""
PRODUCTEUR KAFKA: Open-Meteo → Kafka (Topic: weather-live)
"""

import time
import json
import random
import sys

# Try importing dependencies
try:
    import requests
    from kafka import KafkaProducer
    print("✅ Libraries loaded successfully")
except ImportError as e:
    print(f"❌ Missing libraries: {e}")
    sys.exit(1)

# --------------------------
# CONFIGURATION
# --------------------------
KAFKA_BROKER = "kafka-bd:9092"
TOPIC_NAME = "weather-live"
API_URL = (
    "https://api.open-meteo.com/v1/forecast?"
    "latitude=51.50&longitude=-0.12&current_weather=true"
)

# --------------------------
# INIT KAFKA
# --------------------------
def get_producer():
    max_retries = 10
    for i in range(max_retries):
        try:
            print(f"🔌 Connecting to Kafka ({KAFKA_BROKER})... Attempt {i+1}")
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Connected to Kafka")
            return producer
        except Exception as e:
            print(f"⚠️ Connection failed: {e}")
            time.sleep(5)
    print("❌ Could not connect to Kafka after multiple attempts.")
    return None

# --------------------------
# MAIN LOOP
# --------------------------
def main():
    producer = get_producer()
    if not producer:
        sys.exit(1)

    print(f"🚀 Starting Producer -> Topic: {TOPIC_NAME}")

    while True:
        try:
            # 1. Fetch Data
            r = requests.get(API_URL, timeout=5)
            if r.status_code == 200:
                data = r.json()
                current_weather = data.get("current_weather", {})
                
                # Add timestamp explicitly/enrichment
                current_weather['ingest_time'] = time.time()
                
                # 2. Send to Kafka
                producer.send(TOPIC_NAME, current_weather)
                producer.flush()
                
                print(f"📤 Sent: {current_weather}")
                # Reset backoff on success
                sleep_time = 15
            elif r.status_code == 429:
                print(f"❌ HTTP 429 - Rate limited. Waiting 60s...")
                time.sleep(60)
                continue
            else:
                print(f"⚠️ API Error: {r.status_code}")

        except Exception as e:
            print(f"❌ Loop Error: {e}")

        # Simulate real-time interval (every 15 seconds to avoid rate limiting)
        time.sleep(15)

if __name__ == "__main__":
    main()
