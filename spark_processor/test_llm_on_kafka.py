import json
from kafka import KafkaConsumer
from llm_analyzer import analyze_article

TOPIC = "raw-news"

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="llm-test-reader",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000,  # stop after 5s of no new messages
    )

    print("Reading articles from Kafka and scoring the first 5...\n")

    count = 0
    for message in consumer:
        article = message.value
        count += 1

        print(f"[{count}] {article['title'][:70]}")

        # Send to Gemini for analysis
        try:
            result = analyze_article(article["title"], article.get("summary", ""))
            print(f"    Sentiment: {result['sentiment']} ({result['sentiment_score']})")
            print(f"    Tickers:   {result['tickers']}")
            print()
        except Exception as e:
            print(f"    ERROR: {e}\n")

        # Only score the first 5 to save API calls during testing
        if count >= 5:
            break

    consumer.close()
    print("Done.")

if __name__ == "__main__":
    main()