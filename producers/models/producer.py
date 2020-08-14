"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

BROKER_URLS = "PLAINTEXT://localhost:9092" # Only one host specified in docker compose
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_NAMESPACE = "org.chicago.cta"

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = f"{TOPIC_NAMESPACE}.{topic_name}"
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.admin_client = AdminClient({'bootstrap.servers': BROKER_URLS})

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer({'bootstrap.servers': BROKER_URLS, 'schema.registry.url': SCHEMA_REGISTRY_URL})

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        new_topic = NewTopic(self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)
        self.admin_client.create_topics([new_topic])

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
