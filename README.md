# Retail Inventory System

A data engineering project that tracks product stock levels across a retail store using **Azure Databricks** and **Delta Lake**. It automatically detects low stock and overstock situations and generates business reports using a **Bronze → Silver → Gold** pipeline architecture.

---

## What Does This Project Do?

Imagine you own a retail store with hundreds of products. You need to know:
- Which products are running out of stock?
- Which products have too much stock?
- What is the total stock per category?
- When should you reorder and how much?

This pipeline answers all of these questions automatically by reading raw product data, cleaning it, analyzing it, and storing the results in organized tables.

---
## 🔗 API Source

- API: https://dummyjson.com/products
- Method: GET
- Description: Mock REST API providing product data (id, title, price, stock, category, brand)

### Ingestion via ADF
- Used REST API as source in Azure Data Factory
- Configured HTTP/REST linked service
- Data pulled using Copy Activity
- Stored raw response in ADLS Bronze layer
## Architecture — How Data Flows

```
Raw JSON Files
     |
  BRONZE         →  Store raw data as-is (no changes)
     |
  SILVER         →  Clean data + detect stock problems
     |
  GOLD           →  Business reports + aggregations
     |
  POWER BI       →  Visual dashboard
```

Think of it like a factory:
- **Bronze** = Raw materials coming in
- **Silver** = Materials cleaned and sorted
- **Gold** = Finished product ready to use

---

## Technologies Used

| Technology | What it does in this project |
|---|---|
| Azure Databricks | Runs all the code (like a cloud computer) |
| Azure Data Lake (ADLS) | Stores all the files and tables |
| Delta Lake | Special file format that supports updates and history |
| PySpark | Python library to process large data |
| Azure Data Factory | Schedules the pipeline to run automatically |
| Power BI | Creates visual dashboards from the data |

---

## Project Folder Structure

```
Retail_Inventory_system/
|
├── Logger_files/
│   ├── logger.py               → Sets up logging for all pipelines
│   ├── log_utils.ipynb         → Uploads log files to cloud storage
│   └── decorators.ipynb        → Auto-logs every function (start, end, errors)
|
├── Bronze/
│   └── Bronze_layer.ipynb      → Reads raw JSON and saves to Delta table
|
├── Silver/
│   ├── Silver_main.ipynb       → Main file that runs Silver pipeline
│   ├── Silver_reader.ipynb     → Reads data from Bronze
│   ├── Silver_transformer.ipynb → Cleans data and detects stock issues
│   └── Silver_writer.ipynb     → Saves cleaned data to Delta tables
|
└── Gold/
    └── Gold_layer.ipynb        → Creates business reports and summaries
```

---

## Pipeline Steps — Simple Explanation

### Step 1 — Bronze (Raw Data Ingestion)
- Reads JSON product files from Azure Data Lake
- Flattens nested JSON (products inside arrays)
- Adds extra columns like `ingestion_time` and `source_file`
- Only loads **new files** (skips already loaded ones)
- Saves to `retail_catalog.bronze.retail_table`

### Step 2 — Silver (Data Cleaning + Stock Detection)
- Reads from Bronze table
- **Cleans the data:**
  - Removes rows with no product ID
  - Fills missing brand with "Unknown"
  - Fills missing category with "Uncategorized"
  - Fixes data types (stock as number, price as decimal)
- **Splits into two tables:**
  - `products` → product name, category, brand
  - `inventory` → stock level, price, status
- **Detects stock problems and adds alert details:**
  - `alert_level` → Critical / High / Medium
  - `reorder_qty` → How many units to reorder
  - `alert_time` → When the alert was created
- Uses **Delta MERGE** to update existing records instead of duplicating

### Step 3 — Gold (Business Reports)
- Joins products + inventory tables
- Creates 5 report tables:

| Table | What it shows |
|---|---|
| `inventory_summary` | Total stock and average price per product |
| `category_analysis` | Total stock per category |
| `stock_alerts` | Count of low stock and overstock items |
| `low_stock_detail` | Full details of products that need restocking |
| `category_stock_summary` | Product count, total stock, avg price per category |

---

## Stock Alert Rules

| Stock Level | Alert Level | Reorder Action |
|---|---|---|
| 0 units | Critical | Order 100 units immediately |
| 1 to 3 units | High | Order 75 units urgently |
| 4 to 9 units | Medium | Order 50 units soon |
| More than 100 units | Overstock | Stop ordering, clear existing stock |

---
---

## Email Alerts — Logic App Integration

When low stock products are detected in the Silver pipeline, an **automated email alert** is triggered using **Azure Logic App** with SMTP connector.

### How it works
### What the email contains
- Product name
- Current stock level
- Alert level (Critical / High / Medium)
- Reorder quantity suggested

### Trigger condition
- Any product with `stock < 10` triggers the email
- Alert level decides urgency shown in email subject:
  - `[CRITICAL]` → stock = 0
  - `[HIGH]` → stock 1 to 3
  - `[MEDIUM]` → stock 4 to 9

### Tools used
| Tool | Purpose |
|---|---|
| Azure Logic App | Workflow automation |
| Web connector | Fetches low stock data |
| SMTP connector | Sends email notification |
| Recipient | kumbharsejal24@gmail.com |

| Azure Logic App | Sends automated email alerts for low stock products |

## Incremental Load — How It Works

This pipeline does not reload all data every time. It is smart:

- **First run** → loads all data fresh
- **Second run onwards** → loads only NEW files, updates changed records

This is done using:
- `source_file` column to track which files are already loaded
- Delta MERGE to update stock values without creating duplicates

Example:
```
Run 1: Load products.json         → 30 products inserted
Run 2: Load store_002.json        → 30 new products inserted
Run 3: Load updated_stock.json    → existing products updated with new stock
```

---

## Decorator Pattern

Instead of writing try/except in every single function, a `@log_method` decorator is used:

```python
@log_method
def clean(self, df):
    # just the logic here — no try/except needed
    return df
```

This automatically:
- Logs when the function starts
- Logs when it finishes and how long it took
- Catches and logs any errors

---

## How to Run This Project

### Prerequisites
- Azure Databricks workspace
- Azure Data Lake Storage Gen2
- Unity Catalog with `retail_catalog` created

### Run Order
```
1. Upload JSON file to ADLS Raw_data folder
2. Run Bronze_layer.ipynb
3. Run Silver_main.ipynb
4. Run Gold_layer.ipynb
5. Refresh Power BI dashboard
```

### Verify Everything Worked
```python
print("Bronze   :", spark.read.table("retail_catalog.bronze.retail_table").count())
print("Products :", spark.read.table("retail_catalog.silver.products").count())
print("Inventory:", spark.read.table("retail_catalog.silver.inventory").count())
print("Low stock:", spark.read.table("retail_catalog.silver.low_stock").count())
print("Overstock:", spark.read.table("retail_catalog.silver.over_stock").count())
```

---

## Sample Output

### Low Stock Alerts
| Product Name | Category | Stock | Alert Level | Reorder Qty |
|---|---|---|---|---|
| Dolce Shine Eau de | Fragrances | 4 | Medium | 50 |
| Green Chili Pepper | Groceries | 3 | High | 75 |
| Knoll Conference Chair | Furniture | 0 | Critical | 100 |

### Category Summary
| Category | Total Products | Total Stock | Avg Price |
|---|---|---|---|
| Beauty | 5 | 392 | 13.39 |
| Fragrances | 5 | 280 | 84.00 |
| Furniture | 5 | 340 | 999.99 |
| Groceries | 15 | 749 | 5.89 |

---

## Logs

Every pipeline run saves logs to Azure Data Lake:
```
logs/bronze/BronzeLayer.log
logs/silver/SilverPipeline.log
logs/gold/GoldPipeline.log
```

Log format example:
```
2026-04-06 10:12:31 | SilverPipeline | INFO | Started  : clean
2026-04-06 10:12:31 | SilverPipeline | INFO | Completed: clean in 0.45s
2026-04-06 10:12:32 | SilverPipeline | INFO | Started  : detect_stock
2026-04-06 10:12:32 | SilverPipeline | INFO | Completed: detect_stock in 0.12s
```

---

---

## Project Screenshots

### 1. ADLS Folder Structure
![ADLS Folder Structure](https://github.com/user-attachments/assets/edefd3c3-22dc-434e-9229-c972f8ab7b60)

### 2. Unity Catalog
![Unity Catalog](https://github.com/user-attachments/assets/2ede4199-e4eb-42d6-9ac4-88e903628428)

### 3. Merge History
![Merge History](https://github.com/user-attachments/assets/0a355eac-4e51-407a-b640-b6a0c2a08759)

### 4. Data Factory Pipeline
![Data Factory Pipeline](https://github.com/user-attachments/assets/79cf1573-dc4e-4684-aa2e-02ee7dd7d9a9)

