# Pipeline de Datos con Kafka, HDFS y PostgreSQL en Airflow

## Descripción
- El objetivo del ejercicio es desarrollar un pipeline de ingestión de datos utilizando Kafka, HDFS y PostgreSQL, con Apache Airflow como orquestador.
- Se partirá de un archivo CSV que simulará la generación de datos de sensores, que será procesado y enviado a Kafka para luego almacenarlo en HDFS y consultarlo en PostgreSQL mediante un DAG de Airflow.
- Una vez en PostgreSQL realizar al menos 5 consultas de los datos que puedan dar una visión global de ellos y sacar algunas conclusiones de cara a una empresa que las consuma.
- Datos a Utilizar: https://www.kaggle.com/datasets/alexflorentin/temperature-and-humidity/data

## Qué tareas tendría el DAG
1. Leer el CSV
2. Publicar mensajes en Kafka
3. Esperar a que Kafka Connect escriba en HDFS
4. Verificar que los archivos existen en HDFS
5. Transformar / normalizar los datos
6. Crear tabla en PostgreSQL
7. Cargar los datos en PostgreSQL
8. Ejecutar consultas analíticas
9. Guardar logs o resultados
