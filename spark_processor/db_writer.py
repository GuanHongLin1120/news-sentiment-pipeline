import json
import psycopg2
from kafka import KafkaConsumer

TOPIC = "scored-news"

# PostgreSQL connection settings (matches docker-compose.yml)
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "newsdb",
    "user": "newsuser",
    "password": "newspass",
}

def create_consumer():
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="db-writer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000,  # stop after 10s of no new messages
    )

def insert_article(cursor, article):
    """Insert one scored article into PostgreSQL."""
    cursor.execute(
        """
        INSERT INTO scored_news
            (title, link, summary, published, source, category,
             sentiment, sentiment_score, tickers, ai_summary, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            article.get("title"),
            article.get("link"),
            article.get("summary"),
            article.get("published"),
            article.get("source"),
            article.get("category"),
            article.get("sentiment"),
            article.get("sentiment_score"),
            # tickers is a list — store as comma-separated string
            ",".join(article.get("tickers", [])),
            article.get("ai_summary"),
            article.get("fetched_at"),
        )
    )

def main():
    consumer = create_consumer()
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print(f"Reading from '{TOPIC}' and writing to PostgreSQL...\n")

    count = 0
    for message in consumer:
        article = message.value
        insert_article(cursor, article)
        count += 1
        print(f"[{count}] Saved: {article['title'][:60]}")

    # Commit all inserts at once, then clean up
    conn.commit()
    cursor.close()
    conn.close()
    consumer.close()

    print(f"\nDone. Inserted {count} articles into PostgreSQL.")

if __name__ == "__main__":
    main()