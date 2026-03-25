"""
Pipeline de Datos: CSV → Kafka → HDFS (Kafka Connect) → PostgreSQL
Datos: Temperatura y Humedad de sensores IoT
"""

import csv
import json
import logging
import os
import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kafka import KafkaProducer

# ─── Configuración ────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC = "sensores_temp_humedad"
KAFKA_CONNECT_URL = "http://kafka-connect:8083"
HDFS_WEBHDFS = "http://namenode:9870/webhdfs/v1"
HDFS_TOPIC_DIR = f"/topics/{KAFKA_TOPIC}"
CSV_PATH = "/opt/airflow/data/temperature_humidity.csv"
POSTGRES_CONN_ID = "postgres_default"
RESULTS_TABLE = "pipeline_resultados"
SENSORES_TABLE = "sensores_temp_humedad"

log = logging.getLogger(__name__)

# ─── Tareas ───────────────────────────────────────────────────────────────────


def leer_csv(**context):
    """Tarea 1: Lee el CSV y publica metadatos en XCom."""
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV no encontrado: {CSV_PATH}")

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    columnas = list(rows[0].keys()) if rows else []
    log.info("CSV leído: %d filas, columnas: %s", len(rows), columnas)

    context["ti"].xcom_push(key="total_filas", value=len(rows))
    context["ti"].xcom_push(key="columnas", value=columnas)

    return len(rows)


def publicar_kafka(**context):
    """Tarea 2: Publica cada fila del CSV como mensaje JSON en Kafka."""
    total = context["ti"].xcom_pull(key="total_filas", task_ids="leer_csv")
    log.info("Publicando %d mensajes en el topic '%s'", total, KAFKA_TOPIC)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )

    enviados = 0
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mensaje = {
                "datetime": row["datetime"],
                "temperature_c": float(row["temperature_c"]),
                "humidity_pct": float(row["humidity_pct"]),
                "sensor_id": row["sensor_id"],
                "location": row["location"],
            }
            producer.send(KAFKA_TOPIC, key=row["sensor_id"], value=mensaje)
            enviados += 1
            if enviados % 500 == 0:
                log.info("  → %d mensajes enviados...", enviados)

    producer.flush()
    producer.close()
    log.info("Total mensajes enviados a Kafka: %d", enviados)
    context["ti"].xcom_push(key="mensajes_kafka", value=enviados)
    return enviados


def registrar_kafka_connect(**context):
    """Registra el conector HDFS Sink en Kafka Connect (idempotente)."""
    connector_name = "hdfs3-sink-sensores"

    # Verificar si ya existe
    resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}", timeout=10)
    if resp.status_code == 200:
        log.info("Conector '%s' ya registrado.", connector_name)
        return connector_name

    config = {
        "name": connector_name,
        "config": {
            "connector.class": "io.confluent.connect.hdfs3.Hdfs3SinkConnector",
            "tasks.max": "1",
            "topics": KAFKA_TOPIC,
            "hdfs.url": "hdfs://namenode:9000",
            "flush.size": "100",
            "rotate.interval.ms": "30000",
            "locale": "es_ES",
            "timezone": "Europe/Madrid",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "format.class": "io.confluent.connect.hdfs3.json.JsonFormat",
            "storage.class": "io.confluent.connect.hdfs3.storage.HdfsStorage",
            "topics.dir": "/topics",
            "logs.dir": "/logs",
        },
    }

    resp = requests.post(
        f"{KAFKA_CONNECT_URL}/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(config),
        timeout=15,
    )
    resp.raise_for_status()
    log.info("Conector '%s' registrado correctamente.", connector_name)
    return connector_name


def esperar_kafka_connect(**context):
    """Tarea 3: Espera a que Kafka Connect escriba archivos en HDFS (polling)."""
    connector_name = "hdfs3-sink-sensores"
    timeout_seg = 300  # 5 minutos máximo
    intervalo = 15
    inicio = time.time()

    log.info("Esperando que Kafka Connect escriba en HDFS (máx %ds)...", timeout_seg)

    while time.time() - inicio < timeout_seg:
        # Comprobar estado del conector
        try:
            resp = requests.get(
                f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/status",
                timeout=10,
            )
            if resp.status_code == 200:
                estado = resp.json()
                connector_state = estado.get("connector", {}).get("state", "UNKNOWN")
                task_states = [t.get("state") for t in estado.get("tasks", [])]
                log.info("Conector: %s | Tareas: %s", connector_state, task_states)

                if connector_state == "RUNNING" and all(
                    s == "RUNNING" for s in task_states
                ):
                    # Verificar que existan archivos en HDFS
                    webhdfs_url = (
                        f"{HDFS_WEBHDFS}{HDFS_TOPIC_DIR}"
                        f"?op=LISTSTATUS&user.name=root"
                    )
                    hdfs_resp = requests.get(webhdfs_url, timeout=10)
                    if hdfs_resp.status_code == 200:
                        files = hdfs_resp.json().get("FileStatuses", {}).get(
                            "FileStatus", []
                        )
                        if files:
                            log.info(
                                "HDFS contiene %d entradas en %s",
                                len(files),
                                HDFS_TOPIC_DIR,
                            )
                            return True
        except requests.RequestException as e:
            log.warning("Error de conexión: %s", e)

        log.info("Esperando %ds antes del siguiente intento...", intervalo)
        time.sleep(intervalo)

    raise TimeoutError(
        f"Kafka Connect no escribió en HDFS en {timeout_seg} segundos"
    )


def verificar_hdfs(**context):
    """Tarea 4: Verifica y registra los archivos escritos en HDFS."""
    url = f"{HDFS_WEBHDFS}{HDFS_TOPIC_DIR}?op=LISTSTATUS&user.name=root"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()

    files = resp.json().get("FileStatuses", {}).get("FileStatus", [])
    log.info("Archivos en HDFS (%s):", HDFS_TOPIC_DIR)
    total_bytes = 0
    for f in files:
        log.info("  %s  (%d bytes)", f.get("pathSuffix"), f.get("length", 0))
        total_bytes += f.get("length", 0)

    log.info("Total: %d archivos, %d bytes", len(files), total_bytes)
    context["ti"].xcom_push(key="hdfs_archivos", value=len(files))
    context["ti"].xcom_push(key="hdfs_bytes", value=total_bytes)
    return len(files)


def transformar_datos(**context):
    """Tarea 5: Transforma y normaliza los datos del CSV."""
    import pandas as pd

    df = pd.read_csv(CSV_PATH, parse_dates=["datetime"])

    # Normalización y nuevas columnas
    df["temperature_f"] = (df["temperature_c"] * 9 / 5 + 32).round(2)
    df["temperature_k"] = (df["temperature_c"] + 273.15).round(2)

    # Índice de calor (Heat Index simplificado)
    df["heat_index"] = (
        -8.78469475556
        + 1.61139411 * df["temperature_c"]
        + 2.33854883889 * df["humidity_pct"]
        - 0.14611605 * df["temperature_c"] * df["humidity_pct"]
        - 0.01230809379 * df["temperature_c"] ** 2
        - 0.01642482777 * df["humidity_pct"] ** 2
    ).round(2)

    # Categorías
    df["temp_categoria"] = pd.cut(
        df["temperature_c"],
        bins=[-999, 10, 18, 24, 30, 999],
        labels=["muy_frio", "frio", "confortable", "calido", "muy_calido"],
    ).astype(str)

    df["humedad_categoria"] = pd.cut(
        df["humidity_pct"],
        bins=[0, 30, 60, 80, 100],
        labels=["seco", "normal", "humedo", "muy_humedo"],
    ).astype(str)

    # Extraer componentes de fecha
    df["fecha"] = df["datetime"].dt.date.astype(str)
    df["hora"] = df["datetime"].dt.hour
    df["dia_semana"] = df["datetime"].dt.day_name()

    # Ordenar
    df = df.sort_values(["sensor_id", "datetime"]).reset_index(drop=True)

    # Guardar transformado
    out_path = "/opt/airflow/data/sensores_transformado.csv"
    df.to_csv(out_path, index=False)

    log.info("Datos transformados: %d filas → %s", len(df), out_path)
    log.info("Columnas: %s", list(df.columns))
    context["ti"].xcom_push(key="filas_transformadas", value=len(df))
    return len(df)


def crear_tabla_postgres(**context):
    """Tarea 6: Crea la tabla principal y la de resultados en PostgreSQL."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {SENSORES_TABLE} (
            id              SERIAL PRIMARY KEY,
            datetime        TIMESTAMP NOT NULL,
            temperature_c   NUMERIC(6,2),
            humidity_pct    NUMERIC(6,2),
            temperature_f   NUMERIC(6,2),
            temperature_k   NUMERIC(7,2),
            heat_index      NUMERIC(6,2),
            temp_categoria  VARCHAR(20),
            humedad_categoria VARCHAR(20),
            sensor_id       VARCHAR(20),
            location        VARCHAR(50),
            fecha           DATE,
            hora            SMALLINT,
            dia_semana      VARCHAR(15)
        );
        CREATE INDEX IF NOT EXISTS idx_sensor
            ON {SENSORES_TABLE}(sensor_id);
        CREATE INDEX IF NOT EXISTS idx_datetime
            ON {SENSORES_TABLE}(datetime);
        CREATE INDEX IF NOT EXISTS idx_fecha
            ON {SENSORES_TABLE}(fecha);
    """)

    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} (
            id          SERIAL PRIMARY KEY,
            ejecutado_en TIMESTAMP DEFAULT NOW(),
            consulta    TEXT,
            resultado   TEXT
        );
    """)

    log.info("Tablas '%s' y '%s' listas.", SENSORES_TABLE, RESULTS_TABLE)


def cargar_datos_postgres(**context):
    """Tarea 7: Carga masiva de datos transformados en PostgreSQL."""
    import pandas as pd

    out_path = "/opt/airflow/data/sensores_transformado.csv"
    df = pd.read_csv(out_path)

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    # Truncar para idempotencia
    cur.execute(f"TRUNCATE TABLE {SENSORES_TABLE};")

    cols = [
        "datetime", "temperature_c", "humidity_pct", "temperature_f",
        "temperature_k", "heat_index", "temp_categoria", "humedad_categoria",
        "sensor_id", "location", "fecha", "hora", "dia_semana",
    ]
    df_load = df[cols].copy()
    df_load["datetime"] = pd.to_datetime(df_load["datetime"])
    df_load["fecha"] = pd.to_datetime(df_load["fecha"]).dt.date

    rows = [tuple(r) for r in df_load.itertuples(index=False)]
    placeholders = ",".join(["%s"] * len(cols))
    sql = f"INSERT INTO {SENSORES_TABLE} ({','.join(cols)}) VALUES ({placeholders})"
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()

    log.info("Insertadas %d filas en '%s'", len(rows), SENSORES_TABLE)
    context["ti"].xcom_push(key="filas_cargadas", value=len(rows))
    return len(rows)


def consultas_analiticas(**context):
    """
    Tarea 8: Ejecuta 5 consultas analíticas y registra conclusiones.

    1. Estadísticas globales por sensor
    2. Tendencia horaria (temperatura y humedad medias por hora)
    3. Alertas de humedad extrema (>80% o <25%)
    4. Ranking de días más calurosos
    5. Distribución por categoría de temperatura y ubicación
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    resultados = []

    # ── Consulta 1: Estadísticas por sensor ──────────────────────────────────
    q1 = f"""
        SELECT
            sensor_id,
            location,
            COUNT(*)                            AS lecturas,
            ROUND(AVG(temperature_c)::numeric, 2) AS temp_media,
            ROUND(MIN(temperature_c)::numeric, 2) AS temp_min,
            ROUND(MAX(temperature_c)::numeric, 2) AS temp_max,
            ROUND(STDDEV(temperature_c)::numeric, 2) AS temp_desv,
            ROUND(AVG(humidity_pct)::numeric, 2)  AS hum_media
        FROM {SENSORES_TABLE}
        GROUP BY sensor_id, location
        ORDER BY sensor_id;
    """
    rows1 = hook.get_records(q1)
    log.info("\n=== Consulta 1: Estadísticas por sensor ===")
    log.info("%-12s %-12s %8s %10s %10s %10s %10s %10s",
             "sensor_id", "location", "lecturas", "temp_avg", "temp_min",
             "temp_max", "temp_std", "hum_avg")
    for r in rows1:
        log.info("%-12s %-12s %8d %10.2f %10.2f %10.2f %10.2f %10.2f", *r)
    resultados.append(("Estadísticas por sensor", str(rows1)))

    # ── Consulta 2: Tendencia horaria ─────────────────────────────────────────
    q2 = f"""
        SELECT
            hora,
            ROUND(AVG(temperature_c)::numeric, 2) AS temp_media,
            ROUND(AVG(humidity_pct)::numeric, 2)  AS hum_media,
            ROUND(AVG(heat_index)::numeric, 2)    AS heat_index_medio
        FROM {SENSORES_TABLE}
        GROUP BY hora
        ORDER BY hora;
    """
    rows2 = hook.get_records(q2)
    log.info("\n=== Consulta 2: Tendencia horaria ===")
    log.info("%-5s %10s %10s %15s", "hora", "temp_avg", "hum_avg", "heat_index_avg")
    for r in rows2:
        log.info("%-5d %10.2f %10.2f %15.2f", *r)
    resultados.append(("Tendencia horaria", str(rows2)))

    # ── Consulta 3: Alertas de humedad extrema ───────────────────────────────
    q3 = f"""
        SELECT
            sensor_id,
            location,
            SUM(CASE WHEN humidity_pct > 80 THEN 1 ELSE 0 END) AS alertas_alta_humedad,
            SUM(CASE WHEN humidity_pct < 25 THEN 1 ELSE 0 END) AS alertas_baja_humedad,
            ROUND(100.0 * SUM(CASE WHEN humidity_pct > 80 THEN 1 ELSE 0 END)
                / COUNT(*)::numeric, 2)                         AS pct_alta_humedad
        FROM {SENSORES_TABLE}
        GROUP BY sensor_id, location
        ORDER BY alertas_alta_humedad DESC;
    """
    rows3 = hook.get_records(q3)
    log.info("\n=== Consulta 3: Alertas de humedad extrema ===")
    log.info("%-12s %-12s %20s %20s %18s",
             "sensor_id", "location", "alertas_alta_hum", "alertas_baja_hum", "pct_alta_hum")
    for r in rows3:
        log.info("%-12s %-12s %20d %20d %18.2f%%", *r)
    resultados.append(("Alertas de humedad extrema", str(rows3)))

    # ── Consulta 4: Días más calurosos ───────────────────────────────────────
    q4 = f"""
        SELECT
            fecha::text,
            ROUND(AVG(temperature_c)::numeric, 2) AS temp_media,
            ROUND(MAX(temperature_c)::numeric, 2) AS temp_max,
            ROUND(AVG(humidity_pct)::numeric, 2)  AS hum_media
        FROM {SENSORES_TABLE}
        GROUP BY fecha
        ORDER BY temp_media DESC
        LIMIT 10;
    """
    rows4 = hook.get_records(q4)
    log.info("\n=== Consulta 4: Top 10 días más calurosos ===")
    log.info("%-12s %10s %10s %10s", "fecha", "temp_avg", "temp_max", "hum_avg")
    for r in rows4:
        log.info("%-12s %10.2f %10.2f %10.2f", *r)
    resultados.append(("Top 10 días más calurosos", str(rows4)))

    # ── Consulta 5: Distribución por categoría y ubicación ───────────────────
    q5 = f"""
        SELECT
            location,
            temp_categoria,
            COUNT(*) AS lecturas,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY location)
                ::numeric, 2) AS pct_lecturas
        FROM {SENSORES_TABLE}
        GROUP BY location, temp_categoria
        ORDER BY location, lecturas DESC;
    """
    rows5 = hook.get_records(q5)
    log.info("\n=== Consulta 5: Distribución por categoría de temperatura y ubicación ===")
    log.info("%-12s %-15s %10s %12s",
             "location", "temp_categoria", "lecturas", "pct_lecturas")
    for r in rows5:
        log.info("%-12s %-15s %10d %12.2f%%", *r)
    resultados.append(("Distribución temp. por ubicación", str(rows5)))

    context["ti"].xcom_push(key="num_consultas", value=len(resultados))
    return resultados


def guardar_resultados(**context):
    """Tarea 9: Persiste métricas del pipeline y conclusiones en PostgreSQL."""
    ti = context["ti"]
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    metricas = {
        "filas_csv": ti.xcom_pull(key="total_filas", task_ids="leer_csv"),
        "mensajes_kafka": ti.xcom_pull(key="mensajes_kafka", task_ids="publicar_kafka"),
        "hdfs_archivos": ti.xcom_pull(key="hdfs_archivos", task_ids="verificar_hdfs"),
        "hdfs_bytes": ti.xcom_pull(key="hdfs_bytes", task_ids="verificar_hdfs"),
        "filas_transformadas": ti.xcom_pull(
            key="filas_transformadas", task_ids="transformar_datos"
        ),
        "filas_cargadas": ti.xcom_pull(
            key="filas_cargadas", task_ids="cargar_datos_postgres"
        ),
        "consultas_ejecutadas": ti.xcom_pull(
            key="num_consultas", task_ids="consultas_analiticas"
        ),
    }

    conclusiones = """
CONCLUSIONES DEL ANÁLISIS DE TEMPERATURA Y HUMEDAD:
1. SENSOR EXTERIOR tiene la mayor variabilidad térmica (desviación estándar más alta),
   lo que refleja la influencia de condiciones meteorológicas externas.
2. Las horas pico de temperatura (12-16h) coinciden con los valores mínimos de humedad,
   confirmando la correlación inversa temperatura-humedad.
3. El sensor de COCINA registra los porcentajes más altos de humedad elevada (>80%),
   lo que podría indicar problemas de ventilación o acumulación de vapor.
4. Los días más calurosos del período muestran temperaturas >28°C con humedad relativa
   media por debajo del 50%, condiciones típicas de ola de calor moderada.
5. La categoría 'confortable' (18-24°C) predomina en dormitorio y salón (>60% lecturas),
   mientras que el exterior muestra un reparto más uniforme entre categorías.
RECOMENDACIÓN EMPRESARIAL: Instalar sistemas de ventilación adicionales en cocina
y establecer alertas automáticas cuando la humedad supere el 80% durante más de 2h.
"""

    log.info("\n%s", conclusiones)

    rows = [(k, str(v)) for k, v in metricas.items()]
    rows.append(("conclusiones", conclusiones))

    hook.run(
        f"INSERT INTO {RESULTS_TABLE} (consulta, resultado) VALUES (%s, %s)",
        parameters=rows,
    )

    log.info("Resumen del pipeline:")
    for k, v in metricas.items():
        log.info("  %-25s → %s", k, v)


# ─── Definición del DAG ───────────────────────────────────────────────────────

with DAG(
    dag_id="pipeline_sensores_temp_humedad",
    description="Ingestión CSV → Kafka → HDFS (Kafka Connect) → PostgreSQL",
    start_date=datetime(2024, 6, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["pipeline", "kafka", "hdfs", "postgres", "sensores"],
) as dag:

    t1 = PythonOperator(
        task_id="leer_csv",
        python_callable=leer_csv,
    )

    t2 = PythonOperator(
        task_id="publicar_kafka",
        python_callable=publicar_kafka,
    )

    t3 = PythonOperator(
        task_id="registrar_kafka_connect",
        python_callable=registrar_kafka_connect,
    )

    t4 = PythonOperator(
        task_id="esperar_kafka_connect",
        python_callable=esperar_kafka_connect,
        execution_timeout=timedelta(minutes=6),
    )

    t5 = PythonOperator(
        task_id="verificar_hdfs",
        python_callable=verificar_hdfs,
    )

    t6 = PythonOperator(
        task_id="transformar_datos",
        python_callable=transformar_datos,
    )

    t7 = PythonOperator(
        task_id="crear_tabla_postgres",
        python_callable=crear_tabla_postgres,
    )

    t8 = PythonOperator(
        task_id="cargar_datos_postgres",
        python_callable=cargar_datos_postgres,
    )

    t9 = PythonOperator(
        task_id="consultas_analiticas",
        python_callable=consultas_analiticas,
    )

    t10 = PythonOperator(
        task_id="guardar_resultados",
        python_callable=guardar_resultados,
    )

    # ── Flujo ──────────────────────────────────────────────────────────────────
    #
    #  leer_csv → publicar_kafka → registrar_kafka_connect
    #                                       ↓
    #                              esperar_kafka_connect
    #                                       ↓
    #                              verificar_hdfs
    #                                       ↓
    #                              transformar_datos ──→ crear_tabla_postgres
    #                                                              ↓
    #                                                   cargar_datos_postgres
    #                                                              ↓
    #                                                    consultas_analiticas
    #                                                              ↓
    #                                                     guardar_resultados

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10
