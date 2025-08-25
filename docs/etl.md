# ETL
## COURSE OBJECTIVE
1. Understand what ETL means and why it's importatn in a company's data flow.
2. Identify the phases of an ETL process and how they apply in real-world scenarios.
3. Be able to design a simple ETL pipeline using modern tools.
## COURSE STRUCTURE

**1. Context and Key Concepts (1 hour)**
- What is a data pipeline
- Why do ETL processes exist?
- Differences between ETL and ELT
- Basic architecture of an ETL process
- Real example: "From Excel sales data to a Power BI dashboard"

**2. The Three-Phases: Extract, Transform and Load (1 hour)**

**Extract**
- From files (CSV, JSON)
- From databases (SQL)
- From APIs

**Transform**
- Data cleaning
- Format changes
- Data enrichment and merging

**Load**
- To a relational database
- To a warehouse
- To analytics systems

**3. Hands-on Demo: Building a simple ETL (1-2 hours)**

**Option 1 (low complexity)**
- Extract data from a product CSV
- Clean and normalize names and prices
- Load into SQLite or PostgreSQL

**Option 2 (more engaging)**
- Use Python with pandas + SQLAlchemy
- Visualize the loaded data with a simple dashboard (e.g. Streamlit or PowerBI)
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
### ETL Step 1: Extract – Getting the Data

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

df = pd.read_csv("sales.csv")
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

with open("users.json") as f:
    data = json.load(f)

for user in data:
    print(user["name"])
```

#### 2. Extracting from Databases (SQL)
Data is often stored in relational databases like **PostgreSQL, MySQL, or SQL Server**.

To extract data, you connect to the database and run a **SQL query**.

**Example: Extract sales from a PostgreSQL database**

```python
import psycopg2
import pandas as pd

conn = psycopg2.connect(
    dbname="shop_db",
    user="user",
    password="pass",
    host="localhost"
)

query = "SELECT * FROM sales WHERE date >= '2025-01-01'"
df = pd.read_sql(query, conn)
print(df.head())
```

#### 3. Extracting from APIs

Many modern platforms (e.g., Shopify, Stripe, Google Analytics) offer **APIs** to access their data in real-time.

APIs usually return data in **JSON format**.

**Example: Extract data from a fake weather API**

```python
import requests

response = requests.get("https://api.weatherapi.com/v1/current.json?key=API_KEY&q=London")
data = response.json()

print(data["current"]["temp_c"])
```
#### Summary

| Source Type | Example Tool/Format | How to Extract                           |
| ----------- | ------------------- | ---------------------------------------- |
| Files       | CSV, JSON, Excel    | Use pandas, json or similar libraries    |
| Databases   | PostgreSQL, MySQL   | Connect and query using SQL              |
| APIs        | REST APIs (JSON)    | Use requests to call and parse responses |

After data is extracted, it's still **raw** — messy, inconsistent, maybe incomplete.

Next step: **Transform** it to prepare for analysis.
### ETL Step 2: Transform – Preparing the Data

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

df = pd.read_csv("sales.csv")

# Remove duplicate rows
df = df.drop_duplicates()

# Fill missing prices with 0
df["price"] = df["price"].fillna(0)

# Make all product names lowercase for consistency
df["product"] = df["product"].str.lower()
```
#### 2. Format Changes

Sometimes data needs to be converted to a **standard format** so tools can use it properly.

Common changes include:
- Converting **dates** to standard formats
- Changing **text** to lowercase or uppercase
- Formatting **numbers** (e.g., from strings to floats)

**Example: Convert date format and price type**

```python
# Convert string to datetime
df["date"] = pd.to_datetime(df["date"])

# Convert price to numeric
df["price"] = pd.to_numeric(df["price"])
```
#### 3. Data Enrichment and Merging

Data is more powerful when **combined** with other information.

This can mean:
- Joining two datasets (e.g., sales + customer data)
- Adding calculated fields (e.g., total = price × quantity)
- Adding external info (e.g., currency exchange rates, weather data)

**Example: Merge sales with customer info**

```python
# Load customer data
customers = pd.read_csv("customers.csv")

# Merge sales and customer info using customer ID
df = df.merge(customers, on="customer_id", how="left")
```

**Example: Add a total column**

```python
df["total"] = df["price"] * df["quantity"]
```
#### Summary
| Transformation Type     | Purpose                               | Example       |
| ----------------------- | ------------------------------------- | --------------|
| Data Cleaning           | Fix or remove bad data                | Remove duplicates, fill nulls |
| Format Changes          | Standardize data types and structure  | Dates, numbers, texts |
| Data Enrichment/Merging | Add more value by combining datasets  | Merge tables, create new columns |

After transformation, your data is **clean, consistent, and ready to be used**.

Next step: **Load** it into a destination like a database or dashboard tool.
### ETL Step 3: Load – Storing the Data

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
from sqlalchemy import create_engine
import pandas as pd

# Example transformed data
df = pd.read_csv("clean_sales.csv")

# Connect to PostgreSQL
engine = create_engine("postgresql://user:password@localhost:5432/sales_db")

# Load the data into a table called 'sales_cleaned'
df.to_sql("sales_cleaned", engine, if_exists="replace", index=False)
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

**Example: Load CSV to BigQuery using Python**

```python
from google.cloud import bigquery

client = bigquery.Client()

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
)

with open("clean_sales.csv", "rb") as source_file:
    job = client.load_table_from_file(
        source_file,
        "my_dataset.sales_cleaned",
        job_config=job_config,
    )

job.result()  # Wait for completion
```

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