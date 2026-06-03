import json
import time
import sys
import os
from kafka import KafkaProducer

# Allow importing the scraper modules from the sibling "scraper" folder
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scraper"))

from cnbc_scraper import fetch_cnbc_news
from finnhub_scraper import fetch_company_news

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
# The Kafka topic we will write all raw news into
TOPIC = "raw-news"

def create_producer():
    """Create a Kafka producer that connects to our local Kafka broker."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        # Convert each Python dict into JSON bytes before sending
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def main():
    producer = create_producer()

    # 1. Gather news from both sources
    print("Fetching news from CNBC...")
    cnbc_news = fetch_cnbc_news("technology")

    print("Fetching news from Finnhub (TSM)...")
    finnhub_news = fetch_company_news("TSM")

    all_news = cnbc_news + finnhub_news
    print(f"Total articles to send: {len(all_news)}\n")

    # 2. Send each article into the Kafka topic
    for i, article in enumerate(all_news, start=1):
        producer.send(TOPIC, value=article)
        print(f"[{i}/{len(all_news)}] Sent: {article['title'][:60]}...")
        time.sleep(0.05)  # small delay so we can watch it flow

    # 3. Make sure all messages are actually delivered before exiting
    producer.flush()
    producer.close()
    print("\nAll articles sent to Kafka topic 'raw-news'.")

if __name__ == "__main__":
    main()