from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def generar_numero():
    numero = 7
    print(f"Número generado: {numero}")
    return numero


def consumir_numero(ti=None):
    numero = ti.xcom_pull(task_ids="generar_numero")
    print(f"Número recibido desde XCom: {numero}")
    print(f"Resultado: {numero * 10}")


with DAG(
    dag_id="demo_02_xcom",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo"],
) as dag:

    generar = PythonOperator(
        task_id="generar_numero",
        python_callable=generar_numero,
    )

    consumir = PythonOperator(
        task_id="consumir_numero",
        python_callable=consumir_numero,
    )

    generar >> consumir