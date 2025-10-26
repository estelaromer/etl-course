import os
import time
import psycopg2
from psycopg2 import OperationalError
import pandas as pd
from sqlalchemy import create_engine

USER="testuser"
PASSWORD="testpass"
DATABASE="testdb"
HOST="localhost"
PORT=5432


def get_connection(retries=5, delay=3):
    while retries > 0:
        try:
            conn = psycopg2.connect(
                host=HOST,
                database=DATABASE,
                user=USER,
                password=PASSWORD,
                port=PORT
            )
            print("Connection established.")
            return conn
        except OperationalError as e:
            print("PostgreSQL not ready, retrying...", e)
            retries -= 1
            time.sleep(delay)

    raise Exception("Could not connect to database after several retries")


def load_sales_data(conn, csv_path="./data/sales.csv"):
    df = pd.read_csv(csv_path)

    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            date DATE,
            product TEXT,
            price NUMERIC
        )
    """)
    conn.commit()

    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO sales (date, product, price) VALUES (%s, %s, %s)",
            (row["Date"], row["Product"], row["Price"])
        )

    conn.commit()
    cur.close()
    print(f"Data loaded to table 'sales' from {csv_path}")

def get_engine():
    return create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

def save_dataframe(df, table_name="sales_with_customers", if_exists="replace"):
    engine = get_engine()
    df.to_sql(table_name, engine, index=False, if_exists=if_exists)
    print(f"✅ Dataframe correctly saved into table '{table_name}'.")
