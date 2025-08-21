import os
import time
import psycopg2
from psycopg2 import OperationalError
import pandas as pd

def get_connection(retries=5, delay=3):
    """Intenta conectarse a la base de datos con reintentos automáticos."""
    while retries > 0:
        try:
            conn = psycopg2.connect(
                host=os.environ.get("POSTGRES_HOST", "localhost"),
                database=os.environ.get("POSTGRES_DB", "postgres"),
                user=os.environ.get("POSTGRES_USER", "postgres"),
                password=os.environ.get("POSTGRES_PASSWORD", ""),
                port=os.environ.get("POSTGRES_PORT", "5432")
            )
            print("Conexión a PostgreSQL establecida.")
            return conn
        except OperationalError as e:
            print("PostgreSQL no está listo, reintentando...", e)
            retries -= 1
            time.sleep(delay)

    raise Exception("No se pudo conectar a PostgreSQL después de varios intentos")


def load_sales_data(conn, csv_path="./data/sales.csv"):
    """Crea la tabla 'sales' si no existe y carga los datos desde el CSV."""
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

    # Insertar fila por fila
    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO sales (date, product, price) VALUES (%s, %s, %s)",
            (row["Date"], row["Product"], row["Price"])
        )

    conn.commit()
    cur.close()
    print(f"Datos cargados en la tabla 'sales' desde {csv_path}")
