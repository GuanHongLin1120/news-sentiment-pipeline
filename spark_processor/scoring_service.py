import json
import sys
import os
from kafka import KafkaConsumer, KafkaProducer

# Import our Gemini analyzer (same folder)
from llm_analyzer import analyze_article

INPUT_TOPIC = "raw-news"
OUTPUT_TOPIC = "scored-news"

def create_consumer():
    """Reads raw articles from Kafka."""
    return KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="scoring-service",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # Stop if no new messages arrive for 10 seconds (so the script ends)
        consumer_timeout_ms=10000,
    )

def create_producer():
    """Writes scored articles back to Kafka."""
    return KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def main():
    consumer = create_consumer()
    producer = create_producer()

    print(f"Scoring service started.")
    print(f"Reading from '{INPUT_TOPIC}', writing to '{OUTPUT_TOPIC}'...\n")

    count = 0
    for message in consumer:
        article = message.value
        count += 1

        # 1. Score the article with Gemini
        analysis = analyze_article(article["title"], article.get("summary", ""))

        # 2. Combine the original article with its analysis into one record
        scored_article = {
            **article,                                  # all original fields
            "sentiment": analysis["sentiment"],
            "sentiment_score": analysis["sentiment_score"],
            "tickers": analysis["tickers"],
            "ai_summary": analysis["summary"],
        }

        # 3. Write the enriched record to the scored-news topic
        producer.send(OUTPUT_TOPIC, value=scored_article)

        print(f"[{count}] {analysis['sentiment']:>7} ({analysis['sentiment_score']:+.2f}) | {article['title'][:55]}")

    # Make sure everything is delivered before exiting
    producer.flush()
    producer.close()
    consumer.close()

    print(f"\nDone. Scored and published {count} articles to '{OUTPUT_TOPIC}'.")

if __name__ == "__main__":
    main()