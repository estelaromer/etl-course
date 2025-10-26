# ETL FOUNDAMENTALS
## CONTEXT AND KEY CONCEPTS
### WHAT IS A DATA PIPELINE

A **data pipeline** is a series of steps that move and process data from one system to another.

![Datapipeline](./images/datapipeline.png)

A typical data pipeline has **three main stages**:
1. **Extract** - Get the data
    - From a file, a database, an API or another source
    - Example: sales records from a web store
2. **Transform** - Clean and prepare the data
    - Fix errors, convert formats, filter or enrich
    - Example: remove duplicates, convert dates, calculate totals
3. **Load** - Send the data to its destination
    - A data warehouse, a database or a dashboard tool
    - Example: load sales data into a report in Power BI

#### Why do we need data pipelines?
- To **automate** repetitive data tasks
- To **combine** data from multiple systems
- To **prepare** data for analysis or reporting
- To make sure **data is clean, consistent, and up to date**

Real-World Example

Let’s say a company wants to monitor product sales every day.
1.	Extract sales data from its e-commerce platform (like Shopify)
2.	Transform the data: clean it, convert currencies, calculate daily totals
3.	Load it into a dashboard tool (like Power BI or Looker Studio)

Without a pipeline, someone would have to do this **manually every day**. With a pipeline, it's done **automatically and reliably**.
### WHY DO ETL PROCESSES EXIST

ETL processes exist **to make data more useful**

In most organizations, data comes from many different sources — and **raw data is rarely ready to use**. ETL is the process that turns messy, scattered data into **organized, clean, and meaningful information**.
#### The Main Reasons ETL exist:
1. **Integrating data from multiple sources**

2. **Cleaning and preparing data**

3. **Making data available for analysis**

4. **Automating and scaling data workflows**
### DIFFERENCES BETWEEN ETL AND ELT

ETL and ELT are two **different ways** to move and prepare data, but they have the **same goal**: To get data from a source into a usable format for analysis.
#### ETL - Extract, Transform, Load
- Data is transformed before it is loaded.
- Used with traditional relational databases.
- Transformation is done outside the destination.
- Best for small/medium datasets, complex transformations, used in legacy systems or on-premise setups
#### ELT - Extract, Load, Transform
- Data is transformed after it is loaded.
- Used with modern data warehouses.
- Transformation is done inside the destination.
- Best for large volumes of data, when warehouse has strong compute power, in cloud-first architectures

ETL and ELT are complementary approaches. The choice depends on available tools, data volumes and business needs.
### BASIC ARCHITECTURE OF AN ETL PROCESS

Here’s what a simple ETL pipeline looks like:

![Architecture](./images/etl-diagram.png)

**1. Extract**
- Pulls data from sources like:
    - Databases (SQL server, MySQL)
    - APIs (Stripe, Google Analytics)
    - Files (CSV, Excel, JSON)

**2. Transform**
- Cleans and prepares the data
    - Removes duplicates
    - Fixes formatting
    - Joins different datasets
    - Calculates new metrics

**3. Load**
- Sends the final data to:
    - A database (e.g., PostgreSQL)
    - A data warehouse (e.g., BigQuery, Snowflake)
    - Or even a dashboarding tool

Once loaded, this data is ready to be used by:
- Data analysts
- BI platforms
- Machine learning models

### REAL EXAMPLE: "FROM EXCEL SALES DATA TO A POWER BI DASHBOARD"

Let’s walk through a realistic example of a small business.

#### The Situation:
- A store tracks daily sales in Excel spreadsheets.
- Every day, a new file is created with sales info (product, price, date).
- The manager wants to see total daily sales in a dashboard.

#### ETL process:

**Extract**
- Read multiple Excel files from a shared folder.

**Transform**
- Merge all files into one table
- Clean inconsistent product names
- Convert price fields to numbers
- Group data by date and product
- Calculate total daily revenue

**Load**
- Store the cleaned and structured data in a database (e.g., SQLite or PostgreSQL)
- Connect Power BI to that database

#### Final Result:
- A Power BI dashboard showing:
    - Sales per day
    - Top-selling products
    - Monthly revenue trends
## THE THREE-PHASES: EXTRACT, TRANSFORM AND LOAD
### ETL STEP 1: EXTRACT – GETTING THE DATA

**"Extract"** is the first step in any ETL process.

It means **retrieving raw data** from its original source so we can process it later.

**The goal:**

**Access the data without changing it.**

Just copy or pull the data into your pipeline.
#### 1. Extracting from Files (CSV, JSON)

Many businesses use files to store or exchange data — especially spreadsheets or exports from tools.

**CSV Example:**

You have a file called sales.csv:

```
Date,Product,Price
2025-01-01,Shoes,59.99
2025-01-01,T-shirt,19.99
```

In Python:

```python
import pandas as pd
import os

file = os.path.abspath("data/sales.csv")
df = pd.read_csv(file)
print(df.head())
```
**JSON Example:**

You have a file called users.json:

```json
[
  {"name": "Alice", "email": "alice@example.com"},
  {"name": "Bob", "email": "bob@example.com"}
]
```

In Python:

```python
import json
import os

file = os.path.abspath("data/users.json")
with open(file) as f:
    data = json.load(f)

for user in data:
    print(user["name"])
```

#### 2. Extracting from Databases (SQL)
Data is often stored in relational databases like **PostgreSQL, MySQL, or SQL Server**.

To extract data, you connect to the database and run a **SQL query**.

**Example: Extract sales from a PostgreSQL database**

```python
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
```
```python
# extract-db.py
from db import get_engine
import pandas as pd

engine = get_engine()

query = "SELECT * FROM sales WHERE date >= '2025-01-01'"
df = pd.read_sql(query, engine)
print(df.head())
```

#### 3. Extracting from APIs

Many modern platforms (e.g., Shopify, Stripe, Google Analytics) offer **APIs** to access their data in real-time.

APIs usually return data in **JSON format**.

**Example: Extract data from a fake weather API**

```python
import requests

response = requests.get("https://raw.githubusercontent.com/estelaromer/csv-examples/refs/heads/main/data.json")
data = response.json()

print(data["company"]["name"])
```
#### Summary

| Source Type | Example Tool/Format | How to Extract                           |
| ----------- | ------------------- | ---------------------------------------- |
| Files       | CSV, JSON, Excel    | Use pandas, json or similar libraries    |
| Databases   | PostgreSQL, MySQL   | Connect and query using SQL              |
| APIs        | REST APIs (JSON)    | Use requests to call and parse responses |

After data is extracted, it's still **raw** — messy, inconsistent, maybe incomplete.

Next step: **Transform** it to prepare for analysis.
### ETL STEP 2: TRANSFORM – PREPARING THE DATA

Once data is extracted, it’s often **messy, inconsistent, or incomplete**.

The **Transform** step prepares that raw data for analysis by cleaning it, changing formats, combining sources, and more.

The goal: **Turn raw data into clean, usable data**.
#### 1. Data Cleaning

Real-world data often contains problems:
- Duplicates
- Missing values
- Wrong data types
- Typos or inconsistent values

**Example: Remove duplicates and fill missing values**

```python
import pandas as pd
import os

file = os.path.abspath("data/sales.csv")

df = pd.read_csv(file)

# Remove duplicate rows
df = df.drop_duplicates()

# Fill missing prices with 0
df["Price"] = df["Price"].fillna(0)

# Make all product names lowercase for consistency
df["Product"] = df["Product"].str.lower()

print(df.head())
```
#### 2. Format Changes

Sometimes data needs to be converted to a **standard format** so tools can use it properly.

Common changes include:
- Converting **dates** to standard formats
- Changing **text** to lowercase or uppercase
- Formatting **numbers** (e.g., from strings to floats)

**Example: Convert date format and price type**

```python
import pandas as pd
import os

file = os.path.abspath("data/sales.csv")

df = pd.read_csv(file)

print(df.dtypes)

# Convert string to datetime
df["Date"] = pd.to_datetime(df["Date"])

# Convert price to numeric
df["Price"] = pd.to_numeric(df["Price"])

print(df.dtypes)
```
#### 3. Data Enrichment and Merging

Data is more powerful when **combined** with other information.

This can mean:
- Joining two datasets (e.g., sales + customer data)
- Adding calculated fields (e.g., total = price × quantity)
- Adding external info (e.g., currency exchange rates, weather data)

**Example: Merge sales with customer info**

```python
import pandas as pd
import os

file = os.path.abspath("data/sales.csv")
file2 = os.path.abspath("data/customers.csv")

df = pd.read_csv(file)
df2 = pd.read_csv(file2)

# Merge sales and customer info using customer ID
merged = pd.merge(df, df2, on="Customer_id", how="left")

print(merged.head())
```
#### Summary
| Transformation Type     | Purpose                               | Example       |
| ----------------------- | ------------------------------------- | --------------|
| Data Cleaning           | Fix or remove bad data                | Remove duplicates, fill nulls |
| Format Changes          | Standardize data types and structure  | Dates, numbers, texts |
| Data Enrichment/Merging | Add more value by combining datasets  | Merge tables, create new columns |

After transformation, your data is **clean, consistent, and ready to be used**.

Next step: **Load** it into a destination like a database or dashboard tool.
### ETL STEP 3: LOAD – STORING THE DATA

After the data is **extracted and transformed**, it’s time to load it into its final destination.

The goal: **Save the clean data somewhere useful** — where people, apps, or tools can use it for reporting, analytics, or decision-making.
#### 1. Load to a Relational Database

Relational databases like **PostgreSQL, MySQL, or SQL Server** are common for storing structured data.

They're often used in:
- Internal business apps
- Data integrations
- BI dashboards (connected via SQL)

**Example: Load a DataFrame into PostgreSQL using Python**
```python
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
```
```python
import pandas as pd
import os
from db import get_connection, save_dataframe

# --- Read CSVs ---
file = os.path.abspath("data/sales.csv")
file2 = os.path.abspath("data/customers.csv")

customers = pd.read_csv(file)
sales = pd.read_csv(file2)

# --- Combine dataframes by Customer_id ---
merged = pd.merge(sales, customers, on="Customer_id", how="left")

# --- show first lines ---
print(merged.head())

# --- Saved merged dataframe into PostgreSQL ---
save_dataframe(merged, table_name="sales_with_customers")
```

Once loaded, data analysts or applications can query it using SQL.
#### 2. Load to a Data Warehouse

A **data warehouse** is designed to store **large volumes** of historical, **structured data** optimized for analytics.

Common cloud data warehouses:
- **Google BigQuery**
- **Amazon Redshift**
- **Snowflake**
- **Azure Synapse**

They’re built for:
- Fast querying of big datasets
- Handling transformations at scale (especially in **ELT** setups)
Once loaded, analysts can run SQL queries directly on massive datasets.
#### 3. Load to Analytics or BI Systems

Some ETL processes load data **directly into analytics tools** like:
- **Power BI**
- **Tableau**
- **Looker**
- **Excel (with a connected data source)**

These tools can:
- Connect to databases or warehouses
- Automatically refresh data on a schedule
- Display dashboards, KPIs, charts, and insights

#### Summary
| Destination Types | Used for | Examples |
| ----------------- | -------- | -------- |
| Relational Database | Apps, small/medium BI dashboards | PostgreSQL, MySQL, SQL Server |
| Data Warehouse | Large-scale analytics, ELT | BigQuery, Redshift, Snowflake |
| Analytics / BI Systems | Data visualization and reporting | Tableau, Looker, Power BI | 

After loading, the data is **ready to be explored, queried, and turned into insights**.

### BEST PRACTICES
#### **Error Handling**

Error handling is about **detecting and responding to failures at each stage without corrupting data or silently losing information**.
1. Extract
    - Handle missing files, corrupted formats, or broken connections gracefully
    - Use try/except blocks and validate responses
    - For APIs, handle HTTP errors and timeouts
2. Transform
    - Validate input data before transformation
    - Use try/except blocks to catch and log transformation errors
    - Stop the pipeline if critical fields (e.g., IDs, timestamps) are missing or malformed
3. Load
    - Check if the target table/schema exists before loading
    - Validate data types match destination schema
    - Retry failed insertions (especially in large batches or networks)

```python
# extract_data_bp.py
# Error Handling in Extract phase
import requests

def get_api_data(url):
    """
    Fetch data from a given API endpoint.
    Includes error handling for network issues, HTTP errors, and unexpected responses.
    """
    try:
        # Send a GET request to the API
        response = requests.get(url, timeout=10)
        
        # Raise an exception if the HTTP status code is not 200 (OK)
        response.raise_for_status()

        # Try to parse the response as JSON
        data = response.json()

        # Return the parsed data
        return data

    except requests.exceptions.Timeout:
        # Handle timeout errors (e.g., when the API takes too long to respond)
        print("Error: The request timed out.")
    except requests.exceptions.ConnectionError:
        # Handle network errors (e.g., no internet connection)
        print("Error: Failed to connect to the API.")
    except requests.exceptions.HTTPError as http_err:
        # Handle specific HTTP errors (e.g., 404 Not Found, 500 Internal Server Error)
        print(f"HTTP error occurred: {http_err}")
    except ValueError:
        # Handle errors when parsing JSON fails
        print("Error: The response is not valid JSON.")
    except Exception as err:
        # Catch any other unexpected errors
        print(f"An unexpected error occurred: {err}")

    # Return None if any error occurs
    return None


if __name__ == "__main__":
    # Example API endpoint (public placeholder API)
    api_url = "https://raw.githubusercontent.com/estelaromer/csv-examples/refs/heads/main/data.json"

    # Fetch data from the API
    result = get_api_data(api_url)

    # Check if data was successfully retrieved
    if result is not None:
        print("Data retrieved successfully!")
        # Print the first 3 items for demonstration
        print(result["company"]["name"])
    else:
        print("Failed to retrieve data from the API.")
```
#### **Performance**

Performance is about **moving and transforming data quickly and efficiently while keeping pipelines reliable and scalable**.
1. Extract
    - Read only the necessary data (e.g., use SQL filters: WHERE date >= '2024-01-01')
    - Use streaming or chunking when reading large files or API responses
2. Transform
    - Avoid unnecessary loops (use vectorized operations)
    - Use efficient libraries (e.g., pandas, pyarrow)
    - Profile runtime on large datasets
3. Load
    - Use batch inserts instead of row-by-row
    - Use database indexes for faster queries post-load
    - Avoid full refreshes if only incremental data has changed

```python
# transform_bp_bad.py Bad practice using loops
import numpy as np
import time

# Create a large array of 10 million elements
data = np.arange(10_000_000)

start_time = time.time()

# BAD: Using a Python loop to square each element
squared = []
for value in data:
    squared.append(value ** 2)
squared = np.array(squared)

end_time = time.time()
print(f"Loop version took {end_time - start_time:.4f} seconds")
```

```python
# transform_bp.good.py Best practice using vectorized operation
import numpy as np
import time

# Create a large array of 10 million elements
data = np.arange(10_000_000)

start_time = time.time()

# GOOD: Use NumPy vectorized operation
squared = data ** 2

end_time = time.time()
print(f"Vectorized version took {end_time - start_time:.4f} seconds")
```
#### **Logging**

Logging is about recording what happened and **making pipelines debuggable, auditable and trustworthy**.
1. Extract
    - Log:
        - Source location (file path, URL, DB name)
        - Timestamp of extraction
        - Number of records retrieved
        - Source version, if available
2. Transform
    - Log every transformation step
        - Number of rows before/after filtering
        - Data type changes
        - Rows with missing values
3. Load
    - Log:
        - Number of rows inserted
        - Load timestamps
        - Target table or schema name
        - Whether the load was full or incremental

```python
# logging_bp.py

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

```
#### **Idempotence**

Idempotence means that **running the same operation multiple times has the same end result as running it once — no unintended side effects, no duplicate data, no corruption**.
1. Extract
    - Ensure repeated extractions return consistent results:
        - Use timestamps or IDs to extract only new or changed records
        - Save checkpoints or last successful extraction state
2. Transform
    - Ensure running the transformation twice gives the same result
(e.g., don't duplicate rows if merging)
    - Use consistent column renaming, ordering, and hashing
3. Load
    - Prevent duplicate inserts with primary keys, unique constraints, or upsert strategies (ON CONFLICT DO UPDATE)
    - Use load checkpoints or hashes to detect unchanged data

```python
# idempotence_bq.py

import pandas as pd
from datetime import datetime
import os
import logging

# ----------------------------------------------------
# Logging configuration
# ----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Simulated data source (in a real case, this would be a database or API)
def get_source_data():
    """Simulate a data source with updated_at timestamps."""
    data = [
        {"id": 1, "name": "Alice", "updated_at": "2025-10-25T10:00:00"},
        {"id": 2, "name": "Bob",   "updated_at": "2025-10-26T09:00:00"},
        {"id": 3, "name": "Carol", "updated_at": "2025-10-26T11:00:00"},
    ]
    return pd.DataFrame(data)

# ----------------------------------------------------
# Checkpoint functions
# ----------------------------------------------------
CHECKPOINT_FILE = "checkpoint.txt"

def load_last_checkpoint():
    """Load the last successful extraction timestamp."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            ts = f.read().strip()
            if ts:
                logging.info(f"Loaded last checkpoint: {ts}")
                return datetime.fromisoformat(ts)
    logging.info("No previous checkpoint found. Full extraction will be performed.")
    return None


def save_checkpoint(timestamp: datetime):
    """Save the timestamp of the last successful extraction."""
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(timestamp.isoformat())
    logging.info(f"Saved checkpoint: {timestamp.isoformat()}")


# ----------------------------------------------------
# Extraction logic (idempotent)
# ----------------------------------------------------
def extract_new_data():
    """Extract only new or updated records since the last checkpoint."""
    df = get_source_data()
    last_checkpoint = load_last_checkpoint()

    if last_checkpoint:
        df["updated_at"] = pd.to_datetime(df["updated_at"])
        new_data = df[df["updated_at"] > last_checkpoint]
    else:
        new_data = df

    logging.info(f"Extracted {len(new_data)} new/updated records.")
    return new_data


def main():
    try:
        new_data = extract_new_data()
        if not new_data.empty:
            # Simulate saving to storage or next pipeline step
            logging.info("Processing new records:")
            logging.info(f"\n{new_data}")

            # ✅ Convert updated_at column to datetime if needed
            new_data["updated_at"] = pd.to_datetime(new_data["updated_at"])
            latest_ts = new_data["updated_at"].max()

            # ✅ Ensure latest_ts is a datetime before saving
            if isinstance(latest_ts, str):
                latest_ts = datetime.fromisoformat(latest_ts)

            save_checkpoint(latest_ts)
        else:
            logging.info("No new data to extract.")
    except Exception as e:
        logging.exception(f"Extraction failed: {e}")


if __name__ == "__main__":
    main()
```