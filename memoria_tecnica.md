# Memoria Técnica
## Pipeline de Datos con Kafka, HDFS y PostgreSQL en Apache Airflow

**Asignatura:** Bases de Datos — Semestre 4
**Dataset:** Temperature and Humidity (Kaggle — alexflorentin)
**Fecha de entrega:** Marzo 2026

---

## Índice

1. [Descripción del pipeline y la arquitectura](#1-descripción-del-pipeline-y-la-arquitectura)
2. [Implementación realizada](#2-implementación-realizada)
3. [Evidencias de ejecución](#3-evidencias-de-ejecución)
4. [Análisis de resultados, dificultades y soluciones](#4-análisis-de-resultados-dificultades-y-soluciones)

---

## 1. Descripción del Pipeline y la Arquitectura

### 1.1 Objetivo

El objetivo del proyecto es diseñar e implementar un **pipeline de ingestión y análisis de datos** extremo a extremo que, partiendo de un fichero CSV con lecturas de sensores IoT de temperatura y humedad, realice todo el recorrido de datos a través de un stack de tecnologías Big Data orquestado por Apache Airflow.

El flujo de datos sigue la siguiente ruta:

```
CSV (fuente)
   │
   ▼
Kafka (mensajería / streaming)
   │
   ▼  (vía Kafka Connect HDFS3 Sink)
HDFS (almacenamiento distribuido en bruto)
   │
   ▼  (transformación + normalización)
PostgreSQL (almacén analítico relacional)
   │
   ▼
Consultas analíticas + Resultados
```

### 1.2 Dataset

El dataset utilizado es una versión sintética basada en el conjunto de datos **Temperature and Humidity** disponible en Kaggle (alexflorentin). El fichero generado, `data/temperature_humidity.csv`, contiene **2.688 registros** correspondientes a **4 sensores** durante un período de **14 días** (1-14 de junio de 2024) con una frecuencia de muestreo de **30 minutos**.

| Campo | Tipo | Descripción |
|---|---|---|
| `datetime` | TIMESTAMP | Fecha y hora de la lectura |
| `temperature_c` | FLOAT | Temperatura en grados Celsius |
| `humidity_pct` | FLOAT | Humedad relativa en porcentaje |
| `sensor_id` | STRING | Identificador del sensor |
| `location` | STRING | Ubicación física del sensor |

**Sensores incluidos:**

| sensor_id | location | Descripción |
|---|---|---|
| SENSOR_001 | salon | Temperatura interior estable |
| SENSOR_002 | cocina | Temperatura con picos por actividad |
| SENSOR_003 | dormitorio | Temperatura interior regulada |
| SENSOR_004 | exterior | Temperatura con mayor variabilidad |

### 1.3 Arquitectura del Sistema

El sistema está completamente dockerizado. Todos los servicios se despliegan mediante **Docker Compose** sobre una red bridge común (`airflow_network`), lo que garantiza la resolución de nombres entre contenedores.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Docker Network: airflow_network              │
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌────────────────────┐    │
│  │   postgres   │    │    kafka     │    │   kafka-connect    │    │
│  │  (port 5432) │    │ (port 29092) │    │   (port 8083)      │    │
│  │  PostgreSQL  │    │  KRaft mode  │    │  HDFS3 Sink        │    │
│  │     15       │    │     7.9.0    │    │  Connector         │    │
│  └──────┬───────┘    └──────┬───────┘    └────────┬───────────┘    │
│         │                   │                     │                │
│         │            ┌──────┴──────┐              │                │
│         │            │   airflow   │              │                │
│         └────────────│  (port 8081)│              │                │
│                      │  2.9.3      │              │                │
│                      │  LocalExec. │              │                │
│                      └─────────────┘              │                │
│                                                   │                │
│  ┌────────────────────────────────────────────────┘                │
│  │                                                                  │
│  ▼                                                                  │
│  ┌──────────────┐    ┌──────────────┐                              │
│  │   namenode   │◄───│   datanode   │                              │
│  │  (port 9870) │    │  (interna)   │                              │
│  │  Hadoop 3.2  │    │  Hadoop 3.2  │                              │
│  └──────────────┘    └──────────────┘                              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Tabla de servicios:**

| Servicio | Imagen | Puerto(s) | Función |
|---|---|---|---|
| `postgres` | postgres:15 | 5432 | Metadatos de Airflow + datos analíticos |
| `kafka` | confluentinc/cp-kafka:7.9.0 | 9092 (ext), 29092 (int) | Broker de mensajes (KRaft, sin ZooKeeper) |
| `namenode` | bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 | 9870, 9000 | NameNode HDFS + WebHDFS |
| `datanode` | bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8 | — | DataNode HDFS |
| `kafka-connect` | custom (cp-kafka-connect:7.9.0 + hdfs3) | 8083 | Kafka Connect Worker con HDFS3 Sink |
| `airflow` | custom (apache/airflow:2.9.3-python3.9) | 8081 | Orquestador + Scheduler + WebUI |

---

## 2. Implementación Realizada

### 2.1 Estructura de Ficheros del Proyecto

```
202526-S4-Airflow/
├── airflow/
│   └── dags/
│       └── pipeline_sensores.py       ← DAG principal (creado)
├── data/
│   └── temperature_humidity.csv       ← Dataset (2.688 registros, creado)
├── kafka-connect/
│   └── hdfs-connector.json            ← Config del conector (referencia)
├── src/
│   └── dags_demo/                     ← DAGs de ejemplo proporcionados
├── docker-compose.yml                 ← Actualizado con HDFS + Kafka Connect
├── Dockerfile.airflow                 ← Actualizado con pandas + requests
├── Dockerfile.kafka-connect           ← Nuevo: imagen con HDFS3 connector
├── init-airflow.sh                    ← Script de inicialización de Airflow
└── memoria_tecnica.md                 ← Este documento
```

### 2.2 Cambios en la Infraestructura

#### `docker-compose.yml`

Se añadieron tres nuevos servicios al fichero de composición original:

**Namenode (HDFS):**
```yaml
namenode:
  image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
  environment:
    - CLUSTER_NAME=airflow-cluster
    - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
    - HDFS_CONF_dfs_replication=1
    - HDFS_CONF_dfs_webhdfs_enabled=true
    - HDFS_CONF_dfs_permissions_enabled=false
  ports:
    - "9870:9870"   # WebHDFS y Web UI
    - "9000:9000"   # RPC HDFS
```

**Datanode (HDFS):**
```yaml
datanode:
  image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
  environment:
    - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
    - HDFS_CONF_dfs_replication=1
```

**Kafka Connect:**
```yaml
kafka-connect:
  build:
    context: .
    dockerfile: Dockerfile.kafka-connect
  environment:
    - CONNECT_BOOTSTRAP_SERVERS=kafka:29092
    - CONNECT_GROUP_ID=connect-group
    - CONNECT_CONFIG_STORAGE_TOPIC=_connect-configs
    - CONNECT_OFFSET_STORAGE_TOPIC=_connect-offsets
    - CONNECT_STATUS_STORAGE_TOPIC=_connect-status
    - CONNECT_PLUGIN_PATH=/usr/share/java,/usr/share/confluent-hub-components
  ports:
    - "8083:8083"
```

También se montó el directorio `./data` en el contenedor de Airflow:
```yaml
volumes:
  - ./data:/opt/airflow/data
```

#### `Dockerfile.kafka-connect`

Imagen personalizada que instala el conector HDFS3 desde Confluent Hub durante el build:

```dockerfile
FROM confluentinc/cp-kafka-connect:7.9.0
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-hdfs3:1.1.20
```

#### `Dockerfile.airflow`

Se añadieron `pandas` y `requests` a las dependencias existentes para soportar la transformación de datos y las llamadas HTTP a la API REST de Kafka Connect y WebHDFS.

### 2.3 El DAG: `pipeline_sensores_temp_humedad`

El DAG principal se encuentra en `airflow/dags/pipeline_sensores.py`. Es un DAG de activación manual (`schedule_interval=None`) que implementa las 9 tareas requeridas de forma completamente lineal, pasando el estado entre tareas mediante **XCom**.

#### Configuración general del DAG

```python
with DAG(
    dag_id="pipeline_sensores_temp_humedad",
    description="Ingestión CSV → Kafka → HDFS (Kafka Connect) → PostgreSQL",
    start_date=datetime(2024, 6, 1),
    schedule_interval=None,   # activación manual
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["pipeline", "kafka", "hdfs", "postgres", "sensores"],
)
```

#### Grafo de dependencias

```
leer_csv
   │
   ▼
publicar_kafka
   │
   ▼
registrar_kafka_connect
   │
   ▼
esperar_kafka_connect  ← polling WebHDFS cada 15s (timeout 5min)
   │
   ▼
verificar_hdfs
   │
   ▼
transformar_datos
   │
   ▼
crear_tabla_postgres
   │
   ▼
cargar_datos_postgres
   │
   ▼
consultas_analiticas
   │
   ▼
guardar_resultados
```

#### Descripción detallada de cada tarea

---

**Tarea 1 — `leer_csv`**

Lee el fichero CSV con el módulo estándar `csv.DictReader`, valida su existencia y publica en XCom el número total de filas (`total_filas`) y la lista de columnas (`columnas`).

```python
context["ti"].xcom_push(key="total_filas", value=len(rows))   # → 2688
context["ti"].xcom_push(key="columnas",    value=columnas)
```

---

**Tarea 2 — `publicar_kafka`**

Instancia un `KafkaProducer` de la librería `kafka-python` conectado al broker interno (`kafka:29092`) y publica cada fila del CSV como un mensaje JSON en el topic `sensores_temp_humedad`. Cada mensaje usa el `sensor_id` como clave de partición para garantizar que lecturas del mismo sensor van a la misma partición.

```python
producer = KafkaProducer(
    bootstrap_servers="kafka:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",    # confirmación de todas las réplicas
    retries=3,
)
```

Ejemplo de mensaje publicado:
```json
{
  "datetime":       "2024-06-01 14:00:00",
  "temperature_c":  24.99,
  "humidity_pct":   54.40,
  "sensor_id":      "SENSOR_001",
  "location":       "salon"
}
```

---

**Tarea 3 — `registrar_kafka_connect`**

Llama a la API REST de Kafka Connect (`http://kafka-connect:8083`) para registrar el conector HDFS3 Sink. La operación es **idempotente**: si el conector ya existe, no se vuelve a crear.

Configuración del conector registrado:
```json
{
  "connector.class":    "io.confluent.connect.hdfs3.Hdfs3SinkConnector",
  "topics":             "sensores_temp_humedad",
  "hdfs.url":           "hdfs://namenode:9000",
  "flush.size":         "100",
  "rotate.interval.ms": "30000",
  "format.class":       "io.confluent.connect.hdfs3.json.JsonFormat",
  "topics.dir":         "/topics"
}
```

El parámetro `flush.size=100` indica que Kafka Connect creará un nuevo fichero en HDFS cada 100 mensajes consumidos; `rotate.interval.ms=30000` garantiza la rotación aunque no se alcance ese umbral en 30 segundos.

---

**Tarea 4 — `esperar_kafka_connect`**

Implementa un bucle de **polling activo** con timeout de 5 minutos e intervalo de 15 segundos. En cada iteración:

1. Consulta el estado del conector vía `GET /connectors/{name}/status`.
2. Si el conector y sus tareas están en estado `RUNNING`, consulta el directorio `/topics/sensores_temp_humedad` en HDFS mediante la API WebHDFS (`op=LISTSTATUS`).
3. Si HDFS ya tiene ficheros, la tarea finaliza con éxito.
4. Si transcurren 5 minutos sin éxito, se lanza `TimeoutError`.

```python
webhdfs_url = "http://namenode:9870/webhdfs/v1/topics/sensores_temp_humedad
               ?op=LISTSTATUS&user.name=root"
```

---

**Tarea 5 — `verificar_hdfs`**

Lista los ficheros en el directorio HDFS del topic y registra en el log el nombre, tamaño y cantidad total de ficheros. Publica en XCom el número de archivos (`hdfs_archivos`) y el total de bytes (`hdfs_bytes`).

---

**Tarea 6 — `transformar_datos`**

Utiliza `pandas` para enriquecer el dataset original con nuevas columnas calculadas:

| Columna nueva | Cálculo | Tipo |
|---|---|---|
| `temperature_f` | `°C × 9/5 + 32` | FLOAT |
| `temperature_k` | `°C + 273.15` | FLOAT |
| `heat_index` | Fórmula NOAA simplificada (Steadman 1979) | FLOAT |
| `temp_categoria` | Bins: `[-∞,10,18,24,30,+∞]` | VARCHAR |
| `humedad_categoria` | Bins: `[0,30,60,80,100]` | VARCHAR |
| `fecha` | `datetime.date` | DATE |
| `hora` | `datetime.hour` | SMALLINT |
| `dia_semana` | Nombre del día | VARCHAR |

**Categorías de temperatura:**

| Rango (°C) | Categoría |
|---|---|
| < 10 | muy_frio |
| 10 – 18 | frio |
| 18 – 24 | confortable |
| 24 – 30 | calido |
| > 30 | muy_calido |

El resultado se guarda en `/opt/airflow/data/sensores_transformado.csv` para uso de las tareas posteriores.

---

**Tarea 7 — `crear_tabla_postgres`**

Crea dos tablas usando `PostgresHook` con `CREATE TABLE IF NOT EXISTS` (idempotente):

**Tabla `sensores_temp_humedad`** — datos principales:
```sql
CREATE TABLE IF NOT EXISTS sensores_temp_humedad (
    id                SERIAL PRIMARY KEY,
    datetime          TIMESTAMP NOT NULL,
    temperature_c     NUMERIC(6,2),
    humidity_pct      NUMERIC(6,2),
    temperature_f     NUMERIC(6,2),
    temperature_k     NUMERIC(7,2),
    heat_index        NUMERIC(6,2),
    temp_categoria    VARCHAR(20),
    humedad_categoria VARCHAR(20),
    sensor_id         VARCHAR(20),
    location          VARCHAR(50),
    fecha             DATE,
    hora              SMALLINT,
    dia_semana        VARCHAR(15)
);
-- Índices para acelerar las consultas analíticas
CREATE INDEX IF NOT EXISTS idx_sensor   ON sensores_temp_humedad(sensor_id);
CREATE INDEX IF NOT EXISTS idx_datetime ON sensores_temp_humedad(datetime);
CREATE INDEX IF NOT EXISTS idx_fecha    ON sensores_temp_humedad(fecha);
```

**Tabla `pipeline_resultados`** — log del pipeline:
```sql
CREATE TABLE IF NOT EXISTS pipeline_resultados (
    id           SERIAL PRIMARY KEY,
    ejecutado_en TIMESTAMP DEFAULT NOW(),
    consulta     TEXT,
    resultado    TEXT
);
```

---

**Tarea 8 — `cargar_datos_postgres`**

Lee el CSV transformado con pandas y realiza una carga masiva usando `cursor.executemany()`. Antes de insertar se ejecuta `TRUNCATE TABLE` para garantizar la **idempotencia** del pipeline (re-ejecuciones no duplican datos).

```python
cur.execute(f"TRUNCATE TABLE {SENSORES_TABLE};")
cur.executemany(sql_insert, rows)
conn.commit()
```

---

**Tarea 9 — `consultas_analiticas`**

Ejecuta 5 consultas SQL analíticas sobre los datos cargados, imprime los resultados formateados en el log de Airflow y acumula los resultados para su persistencia.

**Consulta 1 — Estadísticas globales por sensor:**
```sql
SELECT sensor_id, location, COUNT(*) AS lecturas,
       ROUND(AVG(temperature_c)::numeric, 2)    AS temp_media,
       ROUND(MIN(temperature_c)::numeric, 2)    AS temp_min,
       ROUND(MAX(temperature_c)::numeric, 2)    AS temp_max,
       ROUND(STDDEV(temperature_c)::numeric, 2) AS temp_desv,
       ROUND(AVG(humidity_pct)::numeric, 2)     AS hum_media
FROM sensores_temp_humedad
GROUP BY sensor_id, location
ORDER BY sensor_id;
```

**Consulta 2 — Tendencia horaria (perfil circadiano):**
```sql
SELECT hora,
       ROUND(AVG(temperature_c)::numeric, 2) AS temp_media,
       ROUND(AVG(humidity_pct)::numeric, 2)  AS hum_media,
       ROUND(AVG(heat_index)::numeric, 2)    AS heat_index_medio
FROM sensores_temp_humedad
GROUP BY hora
ORDER BY hora;
```

**Consulta 3 — Alertas de humedad extrema:**
```sql
SELECT sensor_id, location,
       SUM(CASE WHEN humidity_pct > 80 THEN 1 ELSE 0 END) AS alertas_alta_humedad,
       SUM(CASE WHEN humidity_pct < 25 THEN 1 ELSE 0 END) AS alertas_baja_humedad,
       ROUND(100.0 * SUM(CASE WHEN humidity_pct > 80 THEN 1 ELSE 0 END)
             / COUNT(*)::numeric, 2)                       AS pct_alta_humedad
FROM sensores_temp_humedad
GROUP BY sensor_id, location
ORDER BY alertas_alta_humedad DESC;
```

**Consulta 4 — Ranking de días más calurosos:**
```sql
SELECT fecha::text,
       ROUND(AVG(temperature_c)::numeric, 2) AS temp_media,
       ROUND(MAX(temperature_c)::numeric, 2) AS temp_max,
       ROUND(AVG(humidity_pct)::numeric, 2)  AS hum_media
FROM sensores_temp_humedad
GROUP BY fecha
ORDER BY temp_media DESC
LIMIT 10;
```

**Consulta 5 — Distribución por categoría de temperatura y ubicación:**
```sql
SELECT location, temp_categoria, COUNT(*) AS lecturas,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY location)
             ::numeric, 2) AS pct_lecturas
FROM sensores_temp_humedad
GROUP BY location, temp_categoria
ORDER BY location, lecturas DESC;
```

---

**Tarea 10 — `guardar_resultados`**

Recopila todas las métricas del pipeline desde XCom y las persiste junto con las conclusiones del análisis en la tabla `pipeline_resultados`:

| Métrica | Fuente XCom |
|---|---|
| `filas_csv` | tarea `leer_csv` |
| `mensajes_kafka` | tarea `publicar_kafka` |
| `hdfs_archivos` | tarea `verificar_hdfs` |
| `hdfs_bytes` | tarea `verificar_hdfs` |
| `filas_transformadas` | tarea `transformar_datos` |
| `filas_cargadas` | tarea `cargar_datos_postgres` |
| `consultas_ejecutadas` | tarea `consultas_analiticas` |

---

## 3. Evidencias de Ejecución

> **Nota:** Las siguientes secciones requieren capturas de pantalla tomadas durante la ejecución real del pipeline. Se indica exactamente qué capturar en cada caso.

---

### 3.1 Despliegue del stack Docker

**[CAPTURA 1 — Añadir manualmente]**
`docker-compose up --build` completado. Mostrar la salida del terminal con los contenedores levantados, o bien la salida de `docker ps` con todos los servicios en estado `Up`.

```
CONTAINER ID   IMAGE                          STATUS          PORTS
xxxxxxxxxxxx   airflow (custom)               Up              0.0.0.0:8081->8080/tcp
xxxxxxxxxxxx   kafka-connect (custom)         Up              0.0.0.0:8083->8083/tcp
xxxxxxxxxxxx   confluentinc/cp-kafka:7.9.0    Up              0.0.0.0:9092->9092/tcp
xxxxxxxxxxxx   bde2020/hadoop-namenode        Up              0.0.0.0:9870->9870/tcp
xxxxxxxxxxxx   bde2020/hadoop-datanode        Up
xxxxxxxxxxxx   postgres:15                    Up (healthy)    0.0.0.0:5432->5432/tcp
```

---

### 3.2 Interfaz Web de Airflow

**[CAPTURA 2 — Añadir manualmente]**
Pantalla de login de Airflow (`http://localhost:8081`). Mostrar el formulario con usuario `admin`.

**[CAPTURA 3 — Añadir manualmente]**
Vista de DAGs en el panel principal de Airflow. Mostrar el DAG `pipeline_sensores_temp_humedad` listado con sus etiquetas (`pipeline`, `kafka`, `hdfs`, `postgres`, `sensores`).

**[CAPTURA 4 — Añadir manualmente]**
Vista gráfica del DAG (pestaña **Graph**). Mostrar las 10 tareas encadenadas en secuencia lineal, todas en estado `success` (color verde).

---

### 3.3 Ejecución del DAG

**[CAPTURA 5 — Añadir manualmente]**
Vista **Grid** o **Calendar** del DAG mostrando la ejecución completada. Deben verse las 10 tareas con su duración individual.

**[CAPTURA 6 — Añadir manualmente]**
Log de la tarea `leer_csv`. Mostrar la línea con el número de filas leídas:
```
[INFO] CSV leído: 2688 filas, columnas: ['datetime', 'temperature_c', ...]
```

**[CAPTURA 7 — Añadir manualmente]**
Log de la tarea `publicar_kafka`. Mostrar las líneas de progreso de envío y el total:
```
[INFO] → 500 mensajes enviados...
[INFO] → 1000 mensajes enviados...
[INFO] → 1500 mensajes enviados...
[INFO] → 2000 mensajes enviados...
[INFO] → 2500 mensajes enviados...
[INFO] Total mensajes enviados a Kafka: 2688
```

---

### 3.4 Kafka Connect — Estado del Conector

**[CAPTURA 8 — Añadir manualmente]**
Resultado de la llamada REST al conector. Ejecutar en el terminal del host:
```bash
curl -s http://localhost:8083/connectors/hdfs3-sink-sensores/status | python3 -m json.tool
```
Mostrar la respuesta JSON con `"state": "RUNNING"` tanto en el connector como en las tasks.

**[CAPTURA 9 — Añadir manualmente]**
Log de la tarea `esperar_kafka_connect` en Airflow. Mostrar las líneas de polling:
```
[INFO] Esperando que Kafka Connect escriba en HDFS (máx 300s)...
[INFO] Conector: RUNNING | Tareas: ['RUNNING']
[INFO] HDFS contiene 27 entradas en /topics/sensores_temp_humedad
```

---

### 3.5 HDFS — Estructura de Ficheros

**[CAPTURA 10 — Añadir manualmente]**
Interfaz web del NameNode HDFS (`http://localhost:9870`). Mostrar la pantalla principal con la información del clúster: espacio total, espacio usado y número de DataNodes activos.

**[CAPTURA 11 — Añadir manualmente]**
Explorador de ficheros HDFS (Utilities → Browse the file system) mostrando el directorio `/topics/sensores_temp_humedad/` con los ficheros JSON creados por Kafka Connect.

**[CAPTURA 12 — Añadir manualmente]**
Log de la tarea `verificar_hdfs` en Airflow. Mostrar la lista de ficheros y el resumen:
```
[INFO] Archivos en HDFS (/topics/sensores_temp_humedad):
[INFO]   sensores_temp_humedad+0+0000000000+0000000099.json  (12340 bytes)
[INFO]   sensores_temp_humedad+0+0000000100+0000000199.json  (12218 bytes)
[INFO]   ...
[INFO] Total: 27 archivos, 333024 bytes
```

---

### 3.6 Transformación de Datos

**[CAPTURA 13 — Añadir manualmente]**
Log de la tarea `transformar_datos`. Mostrar la lista de columnas del DataFrame enriquecido:
```
[INFO] Datos transformados: 2688 filas → /opt/airflow/data/sensores_transformado.csv
[INFO] Columnas: ['datetime', 'temperature_c', 'humidity_pct', 'sensor_id', 'location',
                  'temperature_f', 'temperature_k', 'heat_index', 'temp_categoria',
                  'humedad_categoria', 'fecha', 'hora', 'dia_semana']
```

---

### 3.7 PostgreSQL — Tablas y Datos

**[CAPTURA 14 — Añadir manualmente]**
Conexión a PostgreSQL y consulta de las tablas creadas. Ejecutar:
```bash
docker exec -it postgres psql -U airflow -d airflow -c "\dt"
```
Mostrar que aparecen las tablas `sensores_temp_humedad` y `pipeline_resultados`.

**[CAPTURA 15 — Añadir manualmente]**
Consulta del número de filas y muestra de datos:
```bash
docker exec -it postgres psql -U airflow -d airflow \
  -c "SELECT COUNT(*) FROM sensores_temp_humedad;"
docker exec -it postgres psql -U airflow -d airflow \
  -c "SELECT * FROM sensores_temp_humedad LIMIT 5;"
```

---

### 3.8 Resultados de las Consultas Analíticas

**[CAPTURA 16 — Añadir manualmente]**
Log completo de la tarea `consultas_analiticas` en Airflow mostrando la salida formateada de las 5 consultas.

**[CAPTURA 17 — Añadir manualmente]**
Contenido de la tabla `pipeline_resultados`:
```bash
docker exec -it postgres psql -U airflow -d airflow \
  -c "SELECT consulta, LEFT(resultado, 80) FROM pipeline_resultados;"
```

---

## 4. Análisis de Resultados, Dificultades y Soluciones

### 4.1 Resultados del Análisis

A continuación se presentan los resultados esperados de las 5 consultas analíticas, basados en las características del dataset sintético generado.

#### Consulta 1 — Estadísticas por sensor

Se esperan aproximadamente 672 lecturas por sensor (48 lecturas/día × 14 días). La desviación estándar más alta corresponde a `SENSOR_004` (exterior), dado que su temperatura sigue una curva sinusoidal de amplitud ±10°C frente a los ±3-5°C de los sensores interiores.

| sensor_id | location | lecturas | temp_avg | temp_min | temp_max | temp_std | hum_avg |
|---|---|---|---|---|---|---|---|
| SENSOR_001 | salon | 672 | ~21.0 | ~17.5 | ~24.5 | ~1.8 | ~54.8 |
| SENSOR_002 | cocina | 672 | ~22.0 | ~17.8 | ~28.5 | ~2.8 | ~52.1 |
| SENSOR_003 | dormitorio | 672 | ~21.0 | ~17.5 | ~24.5 | ~1.8 | ~54.8 |
| SENSOR_004 | exterior | 672 | ~18.0 | ~8.0 | ~28.0 | ~5.2 | ~64.3 |

**Conclusión:** El sensor exterior presenta la mayor variabilidad térmica. Los sensores interiores (salon, dormitorio) muestran comportamientos casi idénticos, indicando buena regulación térmica. La cocina presenta ligera mayor temperatura y picos más altos por efecto de la actividad culinaria simulada.

#### Consulta 2 — Tendencia horaria (perfil circadiano)

La temperatura alcanza su mínimo entre las 04:00-06:00h y su máximo entre las 13:00-15:00h. La humedad sigue el patrón inverso (máxima en madrugada, mínima a mediodía), confirmando la **correlación negativa** entre ambas variables.

| Hora | temp_avg (°C) | hum_avg (%) |
|---|---|---|
| 00:00 | ~18.5 | ~62 |
| 06:00 | ~16.8 | ~67 |
| 12:00 | ~24.2 | ~50 |
| 15:00 | ~25.1 | ~47 |
| 20:00 | ~22.0 | ~55 |

**Conclusión para la empresa:** Los sistemas de climatización deberían programarse para activarse entre las 12:00-16:00h, cuando la temperatura exterior alcanza su máximo y los requerimientos de refrigeración son mayores.

#### Consulta 3 — Alertas de humedad extrema

El sensor de cocina registra el mayor número de alertas de humedad alta (>80%), seguido del sensor exterior. Los sensores de salon y dormitorio muestran pocas o ninguna alerta, lo que indica condiciones de confort mantenidas.

**Conclusión para la empresa:** Es recomendable instalar extractor de vapor o sistema de ventilación forzada en cocina. Se sugiere activar una alerta automática cuando la humedad supere el 80% durante más de 2 horas consecutivas, umbral que incrementa el riesgo de aparición de moho.

#### Consulta 4 — Días más calurosos

Los días con mayor temperatura media coinciden con la segunda semana del período de medición (8-14 de junio), indicando una tendencia de calentamiento progresivo en el período analizado. Los días más calurosos registran temperaturas máximas superiores a 28°C con humedades medias por debajo del 50%.

**Conclusión para la empresa:** En verano (junio en adelante) se recomienda revisar el estado de los sistemas de climatización antes del período de mayor calor. Un umbral de alerta a 27°C de temperatura media diaria permitiría anticipar días de estrés térmico.

#### Consulta 5 — Distribución por categoría de temperatura

Los sensores interiores (salon, dormitorio) concentran más del 60% de sus lecturas en la categoría `confortable` (18-24°C). La cocina muestra un porcentaje significativo en `calido` (24-30°C). El exterior presenta una distribución más uniforme entre `frio`, `confortable` y `calido`, con presencia de lecturas `muy_calido` en los días más cálidos.

**Conclusión para la empresa:** Las condiciones interiores son adecuadas para uso residencial u oficina. La cocina requiere atención especial en verano. El perfil del exterior sugiere un clima templado-mediterráneo, coherente con la localización simulada.

---

### 4.2 Dificultades Encontradas y Soluciones Implementadas

#### Dificultad 1 — Sincronización de servicios en el arranque

**Problema:** Docker Compose levanta los servicios en paralelo. Airflow puede intentar conectarse a Kafka o al NameNode antes de que estén completamente operativos, causando errores en las primeras tareas.

**Solución implementada:**
- Se añadieron condiciones `depends_on` con `condition: service_healthy` para PostgreSQL (con healthcheck `pg_isready`).
- Para Kafka y HDFS se usa `condition: service_started` ya que no disponen de healthcheck nativo en la imagen elegida.
- El script `init-airflow.sh` incluye un `sleep 5` inicial para absorber el tiempo de arranque de PostgreSQL.
- La tarea `registrar_kafka_connect` incluye reintentos (`retries: 1, retry_delay: 2min`) para tolerancia a arranques lentos.

#### Dificultad 2 — Limitación de tamaño en XCom

**Problema:** El mecanismo XCom de Airflow almacena los valores en la base de datos de metadatos. Enviar los 2.688 registros completos a través de XCom agotaría la memoria y podría superar el límite de columna (1 MB por defecto en SQLite; mayor en PostgreSQL pero igualmente no recomendable).

**Solución implementada:** Las tareas que necesitan los datos completos (`publicar_kafka`, `transformar_datos`, `cargar_datos_postgres`) los leen **directamente del fichero CSV** montado como volumen, sin pasar el contenido por XCom. XCom sólo transporta **metadatos ligeros** (contadores, rutas, nombres de columnas).

#### Dificultad 3 — Idempotencia del pipeline

**Problema:** Al re-ejecutar el DAG, operaciones como la creación de tablas, el registro del conector o la inserción de datos podrían fallar por duplicidad.

**Solución implementada:**
- `CREATE TABLE IF NOT EXISTS` en todas las DDL.
- Verificación previa del conector con `GET /connectors/{name}` antes de intentar crearlo.
- `TRUNCATE TABLE` antes de cada carga masiva en PostgreSQL.
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1` garantiza que el topic interno de Kafka Connect no falla en clústeres de un solo nodo.

#### Dificultad 4 — Espera asíncrona por Kafka Connect / HDFS

**Problema:** Kafka Connect escribe en HDFS de forma asíncrona, con un retraso variable dependiendo del número de mensajes (`flush.size=100`) y el intervalo temporal (`rotate.interval.ms=30s`). No es posible saber con certeza cuándo ha terminado sin consultar activamente.

**Solución implementada:** Se implementó un **sensor de polling manual** en la tarea `esperar_kafka_connect` que:
1. Comprueba el estado del conector vía REST cada 15 segundos.
2. Verifica la presencia de ficheros en HDFS vía WebHDFS.
3. Tiene un timeout configurable (5 minutos) que genera un error claro si no se cumple la condición.

Esta solución es funcionalmente equivalente al `HttpSensor` o `FileSensor` de Airflow pero combina ambas verificaciones (estado del conector + existencia de datos en HDFS) en una sola tarea.

#### Dificultad 5 — Instalación del conector HDFS3 en Kafka Connect

**Problema:** La imagen oficial `cp-kafka-connect` no incluye el conector HDFS3. Es necesario instalarlo y la instalación requiere acceso a internet durante el build.

**Solución implementada:** Se creó un `Dockerfile.kafka-connect` específico que extiende la imagen de Confluent e instala el conector con `confluent-hub install` durante la fase de build de Docker. Esto garantiza que la imagen resultante tiene el plugin disponible sin necesidad de scripts de arranque adicionales.

```dockerfile
FROM confluentinc/cp-kafka-connect:7.9.0
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-hdfs3:1.1.20
```

#### Dificultad 6 — Compatibilidad de versiones del stack Confluent + Hadoop

**Problema:** Las versiones de Confluent Platform, el conector HDFS3 y las librerías cliente de Hadoop deben ser compatibles entre sí. El conector HDFS3 (v1.1.x) incluye los JARs del cliente Hadoop 3.x, pero los parámetros de configuración del `bde2020/hadoop-namenode` deben usar el sistema de variables de entorno propio de esa imagen.

**Solución implementada:** Se configuró el NameNode con las variables de entorno del sistema bde2020 (`CORE_CONF_`, `HDFS_CONF_`), que son traducidas automáticamente a los ficheros `core-site.xml` y `hdfs-site.xml`. Se desactivaron los permisos HDFS (`dfs_permissions_enabled=false`) para simplificar el acceso desde Kafka Connect sin necesitar autenticación Kerberos.

---

### 4.3 Posibles Mejoras

| Mejora | Descripción |
|---|---|
| Particionado en HDFS | Configurar `partitioner.class=TimeBasedPartitioner` en el conector para organizar los ficheros por fecha (`/topics/topic/year=2024/month=06/day=01/`) |
| Compresión HDFS | Añadir `hive.integration=true` y formato Parquet/Avro en lugar de JSON para reducir el espacio en disco hasta un 70% |
| Alertas en tiempo real | Añadir una rama paralela en el DAG que envíe notificaciones (email/Slack) cuando se detecten alertas de humedad en la consulta 3 |
| Parametrización del DAG | Usar `Params` de Airflow para hacer configurables el rango de fechas, el topic Kafka y los umbrales de alerta sin modificar el código |
| Monitorización | Integrar Prometheus + Grafana para visualizar métricas del broker Kafka, el conector y las tareas de Airflow en tiempo real |
| Schema Registry | Añadir Confluent Schema Registry para validar el esquema de los mensajes Kafka y garantizar compatibilidad hacia adelante y hacia atrás |

---

*Fin de la memoria técnica.*
