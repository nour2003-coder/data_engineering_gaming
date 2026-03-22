from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import json
from config.settings import KAFKA_BROKER, TWITCH_TOPIC

def create_topic(topic_name=TWITCH_TOPIC):
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    topic_list = [NewTopic(name=topic_name, num_partitions=3, replication_factor=1)]
    try:
        admin.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic {topic_name} created successfully.")
    except Exception as e:
        print(f"Topic creation failed or already exists: {e}")

def get_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    return producer