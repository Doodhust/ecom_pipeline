# ECOM Pipeline

## Описание проекта

**ECOM Pipeline** — это пет-проект, предназначенный для генерации и обработки данных о продажах. В этом проекте:

- Генерируются 1 миллион строк данных о продажах.
- Данные загружаются в базу данных PostgreSQL.
- Производится агрегация данных.
- С агрегированными данными производится перенос в ClickHouse для дальнейшего анализа.

## Структура проекта

/home/doodhust/sourse/ecom_pipeline
├── app/
│   ├── __init__.py                # Инициализация пакета
│   ├── generate_sales.py           # Генерация данных о продажах
│   ├── load_postgres.py            # Загрузка данных в PostgreSQL
│   └── transfer_to_clickhouse.py   # Перенос данных в ClickHouse
└── dags/
    └── sales_data_processing.py     # DAG для Apache Airflow

## Основные компоненты

1. **Генерация данных (generate_sales.py)**:
   - Скрипт, который создает 1 миллион строк фиктивных данных о продажах.

2. **Загрузка в PostgreSQL (load_postgres.py)**:
   - Скрипт для вставки сгенерированных данных в базу данных PostgreSQL.

3. **Агрегация и перенос в ClickHouse (transfer_to_clickhouse.py)**:
   - Скрипт, который агрегирует данные в PostgreSQL и переносит их в ClickHouse для оптимизации аналитики.

4. **DAG для Airflow (sales_data_processing.py)**:
   - Apache Airflow управляет процессом от генерации до переноса данных, что позволяет автоматизировать pipeline.