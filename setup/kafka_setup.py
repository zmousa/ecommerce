from confluent_kafka.admin import AdminClient, NewTopic

def create_topics():
    # Define Kafka broker address
    kafka_brokers = ["localhost:9092","localhost:9093"]  # Update with your Kafka broker address

    # Create admin client
    admin_client = AdminClient({'bootstrap.servers': ",".join(kafka_brokers)})

    # Define topics and configurations
    topics = [
        NewTopic("products", num_partitions=1, replication_factor=2),
        NewTopic("customers", num_partitions=1, replication_factor=2),
        NewTopic("orders", num_partitions=1, replication_factor=2),
        NewTopic("order_items", num_partitions=1, replication_factor=2)
    ]

    # Create topics
    futures = admin_client.create_topics(topics)

    # Wait for topics to be created
    for topic, future in futures.items():
        try:
            future.result()  # Raise exception if creation failed
            print(f"Topic {topic} created successfully")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")

if __name__ == "__main__":
    create_topics()
