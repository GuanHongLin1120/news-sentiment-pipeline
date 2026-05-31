import json
from kafka import KafkaConsumer

# The topic we want to read from
TOPIC = "raw-news"

def create_consumer():
    """Create a Kafka consumer that reads from the raw-news topic."""
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        # Start reading from the very beginning of the topic
        auto_offset_reset="earliest",
        # A name for this consumer group
        group_id="news-reader",
        # Convert the incoming JSON bytes back into a Python dict
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

def main():
    consumer = create_consumer()
    print(f"Listening to topic '{TOPIC}'... (Ctrl+C to stop)\n")

    count = 0
    for message in consumer:
        article = message.value
        count += 1
        print(f"[{count}] {article['source'].upper()} | {article['title'][:60]}")

if __name__ == "__main__":
    main()