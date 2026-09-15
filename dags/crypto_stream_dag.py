from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
KAFKA_TOPIC = "crypto_prices"
KAFKA_BROKER = "kafka:9092"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

import json

import requests
from kafka import KafkaProducer


def fetch_and_publish():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for symbol in SYMBOLS:
        response = requests.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": symbol},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        message = {
            "symbol": data["symbol"],
            "price": float(data["price"]),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }


        future = producer.send(KAFKA_TOPIC, value=message)
        future.get(timeout=10)  # bloque et lève une exception si l'envoi échoue vraiment
        print(f"Publié : {message}")

    producer.flush()
    producer.close()

with DAG(
    dag_id="crypto_price_stream",
    default_args=default_args,
    description="Récupère les prix crypto depuis Binance et les publie dans Kafka",
    schedule=timedelta(minutes=1),
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["crypto", "kafka", "streaming"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_and_publish_prices",
        python_callable=fetch_and_publish,
    )