import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


def load_env(dotenv_path=".env.local"):
    """Load simple KEY=VALUE lines from a .env file."""
    env = {}
    if not os.path.exists(dotenv_path):
        return env

    with open(dotenv_path, encoding="utf-8") as dotenv_file:
        for line in dotenv_file:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def get_env_setting(env, *keys):
    for key in keys:
        if key in env and env[key]:
            return env[key]
        lower_key = key.lower()
        if lower_key in env and env[lower_key]:
            return env[lower_key]
    return None


def create_producer(bootstrap_servers, api_key=None, api_secret=None, resource=None):
    """Create and return a Kafka producer, optionally using SASL credentials."""
    config = {
        "bootstrap_servers": bootstrap_servers,
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
        "key_serializer": lambda k: k.encode("utf-8") if isinstance(k, str) else None,
    }

    if api_key and api_secret:
        config.update(
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username=api_key,
            sasl_plain_password=api_secret,
        )
        if resource:
            config["client_id"] = resource

    return KafkaProducer(**config)


def generate_message(index):
    """Generate a single event message with a defined JSON schema."""
    event_types = [
        "order_created",
        "order_updated",
        "order_shipped",
        "order_cancelled",
        "inventory_adjusted",
    ]
    regions = ["us-east-1", "us-west-2", "eu-central-1", "ap-southeast-2"]
    statuses = ["pending", "processing", "completed", "cancelled"]

    return {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "event_type": random.choice(event_types),
        "source": "delta-lake-producer",
        "region": random.choice(regions),
        "payload": {
            "order_id": f"order-{100000 + index}",
            "customer_id": f"cust-{random.randint(1000, 9999)}",
            "product_id": f"prod-{random.randint(100, 999)}",
            "quantity": random.randint(1, 20),
            "price": round(random.uniform(10.0, 500.0), 2),
            "status": random.choice(statuses)
        },
    }


def generate_messages(count):
    """Build a batch of JSON messages."""
    return [generate_message(i) for i in range(count)]


def produce_messages(producer, topic, messages):
    """Send a list of messages to the given Kafka topic."""
    for idx, message in enumerate(messages):
        key = f"key-{idx}"
        producer.send(topic, key=key, value=message)
    producer.flush()


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka producer with random JSON payloads")
    parser.add_argument("--env-file", default="../.env.local", help="Path to .env file containing Kafka credentials")
    parser.add_argument(
        "--bootstrap-servers",
        default=None,
        help="Comma-separated list of Kafka bootstrap servers",
    )
    parser.add_argument("--api-key", help="Kafka API key for SASL auth")
    parser.add_argument("--api-secret", help="Kafka API secret for SASL auth")
    parser.add_argument("--resource", help="Optional resource/client id")
    parser.add_argument("--topic", required=True, help="Kafka topic to produce to")
    parser.add_argument(
        "--count",
        type=int,
        default=1000,
        help="Number of random messages to produce",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    env = load_env(args.env_file)

    bootstrap_servers = get_env_setting(env, "server", "BOOTSTRAP_SERVERS") 

    api_key = args.api_key or get_env_setting(env, "api_key", "API_KEY", "KAFKA_API_KEY")
    api_secret = args.api_secret or get_env_setting(env, "api_secret", "API_SECRET", "KAFKA_API_SECRET")
    resource = args.resource or get_env_setting(env, "resource", "RESOURCE", "KAFKA_RESOURCE")

    if not bootstrap_servers:
        raise ValueError("Bootstrap servers must be provided via --bootstrap-servers or .env")

    producer = create_producer(
        bootstrap_servers=bootstrap_servers.split(","),
        api_key=api_key,
        api_secret=api_secret,
        resource=resource,
    )
    connected = producer.bootstrap_connected()
    print(f"Bootstrap connected: {connected}")
    try:
        partitions = producer.partitions_for(args.topic)
        print(f"Topic metadata fetched: {'yes' if partitions is not None else 'no'}")
    except Exception as exc:
        print(f"Error fetching topic metadata: {exc}")
    if not connected:
        print("Warning: producer has not yet connected to bootstrap servers.")
    
    # Generate all messages
    all_messages = generate_messages(args.count)
    
    # Send first batch: 910 messages
    print(f"\nSending first batch: 910 messages...")
    produce_messages(producer, args.topic, all_messages[:910])
    print(f"Produced 910 messages to topic '{args.topic}'")
    
    # Wait 2 minutes
    print("\nWaiting 2 minutes before sending second batch...")
    time.sleep(120)
    
    # Send second batch: 90 messages
    print(f"Sending second batch: 90 messages...")
    produce_messages(producer, args.topic, all_messages[910:1000])
    print(f"Produced 90 messages to topic '{args.topic}'")
    
    print(f"\nTotal: {len(all_messages)} messages produced in 2 batches")


if __name__ == "__main__":
    main()


