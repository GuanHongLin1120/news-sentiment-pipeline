import json
import sys
import os
import time
from kafka import KafkaConsumer, KafkaProducer

# Import our Gemini analyzer (same folder)
from llm_analyzer import analyze_article

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
INPUT_TOPIC = "raw-news"
OUTPUT_TOPIC = "scored-news"

def create_consumer():
    """Reads raw articles from Kafka."""
    return KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        group_id="scoring-service",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

def create_producer():
    """Writes scored articles back to Kafka."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def main():
    consumer = create_consumer()
    producer = create_producer()

    print("Scoring service started (long-running).")
    print(f"Listening on '{INPUT_TOPIC}', publishing to '{OUTPUT_TOPIC}'. Waiting for messages...\n")

    count = 0
    # This loop now runs forever — it blocks and waits for new messages
    for message in consumer:
        article = message.value
        count += 1

        analysis = analyze_article(article["title"], article.get("summary", ""))

        scored_article = {
            **article,
            "sentiment": analysis["sentiment"],
            "sentiment_score": analysis["sentiment_score"],
            "tickers": analysis["tickers"],
            "ai_summary": analysis["summary"],
        }

        producer.send(OUTPUT_TOPIC, value=scored_article)
        producer.flush()  # flush each message since the service runs continuously

        print(f"[{count}] {analysis['sentiment']:>7} ({analysis['sentiment_score']:+.2f}) | {article['title'][:55]}")
        time.sleep(2.5)  # throttle to stay under Groq's 30 req/min free-tier limit

if __name__ == "__main__":
    main()