import pandas as pd
import psycopg2
from psycopg2 import sql
import logging
import os
import sys

# ----------------------------------------------------
# Logging configuration
# ----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("csv_to_postgres.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# ----------------------------------------------------
# Database connection parameters
# ----------------------------------------------------
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "testdb",
    "user": "testuser",
    "password": "testpass"
}

# ----------------------------------------------------
# CSV file and table name
# ----------------------------------------------------
CSV_PATH = os.path.abspath("data/sales.csv")
TABLE_NAME = "new_sales"


def main():
    try:
        # ----------------------------------------------------
        # Step 1: Read CSV
        # ----------------------------------------------------
        logging.info(f"Reading data from CSV: {CSV_PATH}")
        df = pd.read_csv(CSV_PATH)
        logging.info(f"Retrieved {len(df)} records from CSV.")

        if df.empty:
            logging.warning("CSV file is empty. Nothing to insert.")
            return

        # ----------------------------------------------------
        # Step 2: Connect to PostgreSQL
        # ----------------------------------------------------
        logging.info("Connecting to PostgreSQL database...")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        logging.info("Database connection established successfully.")

        # ----------------------------------------------------
        # Step 3: Insert data
        # ----------------------------------------------------
        cols = list(df.columns)
        insert_query = sql.SQL("""
            INSERT INTO {table} ({fields})
            VALUES ({placeholders})
        """).format(
            table=sql.Identifier(TABLE_NAME),
            fields=sql.SQL(', ').join(map(sql.Identifier, cols)),
            placeholders=sql.SQL(', ').join(sql.Placeholder() * len(cols))
        )

        rows_inserted = 0
        for row in df.itertuples(index=False, name=None):
            try:
                cur.execute(insert_query, row)
                rows_inserted += 1
            except Exception as e:
                logging.error(f"Error inserting row {row}: {e}")

        conn.commit()
        logging.info(f"Inserted {rows_inserted} rows into '{TABLE_NAME}'.")

    except FileNotFoundError:
        logging.error(f"CSV file not found: {CSV_PATH}")
    except psycopg2.OperationalError as e:
        logging.error(f"Database connection failed: {e}")
    except Exception as e:
        logging.exception(f"Unexpected error: {e}")
    finally:
        # ----------------------------------------------------
        # Step 4: Cleanup
        # ----------------------------------------------------
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
                logging.info("Database connection closed.")
        except Exception:
            pass


if __name__ == "__main__":
    main()
