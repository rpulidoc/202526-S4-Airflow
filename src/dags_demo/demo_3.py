import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "demo_airflow_topic"


def enviar_mensajes():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    mensajes = [
        {"id": 1, "mensaje": "hola"},
        {"id": 2, "mensaje": "airflow"},
        {"id": 3, "mensaje": "kafka"},
    ]

    for mensaje in mensajes:
        producer.send(KAFKA_TOPIC, value=mensaje)
        print(f"Enviado: {mensaje}")

    producer.flush()
    producer.close()

    print("Mensajes enviados correctamente")


with DAG(
    dag_id="demo_03_kafka",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["demo", "kafka"],
) as dag:

    enviar = PythonOperator(
        task_id="enviar_mensajes_kafka",
        python_callable=enviar_mensajes,
    )