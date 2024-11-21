from clickhouse_driver import Client
import psycopg2
from datetime import datetime, timedelta
import csv


client = Client(host='localhost', port=9000, user='clickhouse_user', password='clickhouse_password', database='default')

create_table_query = """
CREATE TABLE IF NOT EXISTS sales (
    region String,
    product_id Int32,
    total_sales Int32,
    average_sale_amount Decimal(10, 2),
    import_date DateTime
) ENGINE = MergeTree()
ORDER BY import_date;
"""

client.execute(create_table_query)

client.execute(""" SELECT count(*) FROM sales;
""")

postgres_conn = psycopg2.connect(
    dbname='test',
    user='user',
    password='password',
    host='localhost',
    port=5432
)

clickhouse_client = Client(
    host='localhost',
    port=9000,
    user='clickhouse_user',
    password='clickhouse_password',
    database='default'
)


postgres_cursor = postgres_conn.cursor()
postgres_cursor.execute("SELECT region, product_id, total_sales, average_sale_amount FROM aggregation_sales")

rows = postgres_cursor.fetchall()

current_import_date = datetime.now()

data_to_insert = [(region, product_id, total_sales, average_sale_amount, current_import_date) for region, product_id, total_sales, average_sale_amount in rows]

clickhouse_client.execute(
    "INSERT INTO sales (region, product_id, total_sales, average_sale_amount, import_date) VALUES",
    data_to_insert
)

postgres_cursor.close()
postgres_conn.close()
clickhouse_client.disconnect()


conn = psycopg2.connect(
    dbname='test',
    user='user',
    password='password',
    host='localhost',
    port=5432
)
cursor = conn.cursor()

try:
    cursor.execute("""
    DROP TABLE sales
    """)

    conn.commit()


except psycopg2.Error as e:
    print(f"Ошибка: {e}")