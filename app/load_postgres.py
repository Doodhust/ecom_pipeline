import psycopg2
from psycopg2 import sql
import pandas as pd
from psycopg2.extras import execute_values
import os



df = pd.read_csv('sales_data.csv')
csv_file_path = 'sales_data.csv'

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
    CREATE TABLE IF NOT EXISTS sales (
        sale_id SERIAL PRIMARY KEY,
        customer_id INT,
        product_id INT,
        quantity INT,
        sale_date TIMESTAMP,
        region VARCHAR(50)
    )
    """)

    tuples = [tuple(x) for x in df.to_numpy()]

    cols = df.columns.tolist()
    query = sql.SQL("INSERT INTO sales ({}) VALUES %s").format(
        sql.SQL(', ').join(map(sql.Identifier, cols))
    )

    execute_values(cursor, query, tuples) # вставка

    conn.commit()

    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)
        print(f"Файл {csv_file_path} был успешно удалён.")

except psycopg2.Error as e:
    print(f"Ошибка: {e}")
    conn.rollback()  # Откат транзакции

finally:
    cursor.close()
    conn.close()


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
    SELECT * FROM sales
    LIMIT 10
    """)
    rows = cursor.fetchall()
    for row in rows:
        print(row)

    

except psycopg2.Error as e:
    print(f"Ошибка: {e}")

finally:
    cursor.close()
    conn.close()

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
    CREATE TABLE IF NOT EXISTS aggregation_sales as SELECT
    region, 
    product_id, 
    SUM(quantity) AS total_sales, 
    AVG(quantity) AS average_sale_amount
    FROM 
        sales
    GROUP BY 
        region, 
        product_id
    ORDER BY 
        region, 
        product_id;
""")
    
    conn.commit()
    
    cursor.execute("""
    SELECT * FROM aggregation_sales
    LIMIT 10
    """)
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    

except psycopg2.Error as e:
    print(f"Ошибка: {e}")