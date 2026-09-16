import json
import sys
import time

from cassandra.cluster import Cluster
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "crypto_prices"
CASSANDRA_HOST = "cassandra"


def produce_test_message():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    message = {
        "symbol": "BTCUSDT",
        "price": 99999.99,
        "fetched_at": "2024-01-01T00:00:00+00:00",
    }
    producer.send(TOPIC, message)
    producer.flush()
    print("Message envoyé dans Kafka:", message)


def check_cassandra():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect("crypto_keyspace")
    rows = session.execute(
        "SELECT * FROM prices WHERE symbol = 'BTCUSDT' ALLOW FILTERING"
    )
    found = list(rows)
    cluster.shutdown()
    return len(found) > 0


if __name__ == "__main__":
    produce_test_message()

    print("Attente que Spark traite le message...")
    for attempt in range(12):
        time.sleep(10)
        if check_cassandra():
            print("Message retrouvé dans Cassandra — pipeline OK")
            sys.exit(0)
        print(f"Tentative {attempt + 1}/12: pas encore trouvé")

    print("ÉCHEC: message jamais retrouvé dans Cassandra")
    sys.exit(1)