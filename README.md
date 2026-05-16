# DS206 Project 2 - Group 1 DDS Pipeline

This project creates and populates the `ORDER_DDS` dimensional data store from `raw_data_source.xlsx`.

## Required One-Time Setup

Before running the Python pipeline, run the SQL files in `infrastructure_initiation/` once in SQL Server, in this order:

1. `infrastructure_initiation/dimensional_database_creation.sql`
2. `infrastructure_initiation/staging_raw_table_creation.sql`
3. `infrastructure_initiation/dimensional_db_table_creation.sql`

These scripts create the database, source/staging tables, dimensions, fact table, fact error table, `Dim_SOR`, keys, and constraints. They are not part of the runtime pipeline because table initiation should be done once before data loads.

The professor or grader should only need to:

1. Adjust `infrastructure_initiation/sql_server_config.cfg` if their SQL Server host, port, user, driver, or auth mode differs.
2. Run the three DDL files above once.

## Configuration

Shared non-secret defaults are stored in:

```text
infrastructure_initiation/sql_server_config.cfg
```

Local secrets are stored in `.env`, which is ignored by Git. Keep database configuration in `infrastructure_initiation/sql_server_config.cfg`; `.env` should only contain secret values such as passwords.

Copy the example:

```bash
cp .env.example .env
```

Minimum `.env` for SQL authentication:

```env
MSSQL_PASSWORD=your_password
```

Example `sql_server_config.cfg` for SQL authentication:

```ini
[sql_server]
driver = ODBC Driver 18 for SQL Server
server = localhost
port = 1433
database = ORDER_DDS
trusted_connection = no
encrypt = yes
trust_server_certificate = yes
user = sa
password =
```

Example `sql_server_config.cfg` for Windows trusted authentication with local SQL Server Express:

```ini
[sql_server]
driver = ODBC Driver 18 for SQL Server
server = localhost\SQLEXPRESS
port = 1433
database = ORDER_DDS
trusted_connection = yes
encrypt = no
trust_server_certificate = yes
user =
password =
```

The connection helper supports both styles. SQL authentication uses `pymssql` first for compatibility with the original project setup, then falls back to ODBC if a driver is configured. Windows trusted authentication uses ODBC because `pymssql` does not support that login style reliably.

## Runtime Pipeline

Run the pipeline after the one-time SQL setup:

```bash
python main.py --start_date=1996-01-01 --end_date=1998-12-31
```

`main.py` only parses `start_date` and `end_date`, creates `DimensionalDataFlow`, and calls `exec()`.

The flow executes runtime tasks in this order:

1. Load all Excel sheets into the source/staging tables named `Categories`, `Customers`, `Employees`, `OrderDetails`, `Orders`, `Products`, `Region`, `Shippers`, `Suppliers`, and `Territories`.
2. Update dimension tables.
3. Update `FactOrders` for the selected date range.
4. Update `FactOrders_Error` for rejected rows in the selected date range.

The Python pipeline does not create or drop database tables.

## Project Configuration

Non-secret project paths, table names, query names, and load order are centralized in:

```text
pipeline_dimensional_data/config.py
```

Runtime code should not hard-code project paths outside this config module.

## Logging

Pipeline logs are written at `INFO` level to:

```text
logs/logs_dimensional_data_pipeline.txt
```

Each log line includes the pipeline `execution_id` UUID.

## Tests

Run:

```bash
python -m pytest -q
```

The tests use mocks and temporary files. They do not require a live SQL Server connection.

## Main Files

```text
main.py
utils.py
logging.py
pipeline_dimensional_data/config.py
pipeline_dimensional_data/flow.py
pipeline_dimensional_data/tasks.py
infrastructure_initiation/
pipeline_dimensional_data/queries/
tests/test_utils.py
```
