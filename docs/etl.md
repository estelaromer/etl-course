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