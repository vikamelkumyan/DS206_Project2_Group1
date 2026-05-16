import os
import sys

import pandas as pd
import pymssql

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import execute_sql_script, load_sql_script, parse_db_config


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INFRASTRUCTURE_DIR = os.path.join(PROJECT_ROOT, "infrastructure_initiation")
QUERIES_DIR = os.path.join(PROJECT_ROOT, "pipeline_dimensional_data", "queries")

SCHEMA_NAME = "dbo"

DIMENSION_LOAD_ORDER = [
    {
        "name": "categories",
        "query": "update_dim_categories.sql",
        "source_table_name": "staging_raw_Categories",
        "target_table_name": "DimCategories",
    },
    {
        "name": "region",
        "query": "update_dim_region.sql",
        "source_table_name": "staging_raw_Region",
        "target_table_name": "DimRegion",
    },
    {
        "name": "shippers",
        "query": "update_dim_shippers.sql",
        "source_table_name": "staging_raw_Shippers",
        "target_table_name": "DimShippers",
    },
    {
        "name": "suppliers",
        "query": "update_dim_suppliers.sql",
        "source_table_name": "staging_raw_Suppliers",
        "target_table_name": "DimSuppliers",
    },
    {
        "name": "employees",
        "query": "update_dim_employees.sql",
        "source_table_name": "staging_raw_Employees",
        "target_table_name": "DimEmployees",
    },
    {
        "name": "customers",
        "query": "update_dim_customers.sql",
        "source_table_name": "staging_raw_Customers",
        "target_table_name": "DimCustomers",
    },
    {
        "name": "territories",
        "query": "update_dim_territories.sql",
        "source_table_name": "staging_raw_Territories",
        "target_table_name": "DimTerritories",
    },
    {
        "name": "products",
        "query": "update_dim_products.sql",
        "source_table_name": "staging_raw_Products",
        "target_table_name": "DimProducts",
    },
]

STAGING_SHEETS = [
    "Categories",
    "Customers",
    "Employees",
    "OrderDetails",
    "Orders",
    "Products",
    "Region",
    "Shippers",
    "Suppliers",
    "Territories",
]


def _result(success, task_name, **extra):
    result = {"success": success, "task": task_name}
    result.update(extra)
    return result


def connect_to_db(database=None):
    cfg = parse_db_config()
    if cfg is None:
        raise ValueError("SQL Server configuration could not be loaded.")

    connection_database = database or cfg["database"]
    user = cfg.get("user") or None
    password = cfg.get("password") or None

    return pymssql.connect(
        server=cfg["server"],
        user=user,
        password=password,
        database=connection_database,
        port=cfg["port"],
    )


def _format_sql(sql_script, parameters):
    try:
        return sql_script.format(**parameters)
    except KeyError as exc:
        missing_key = exc.args[0]
        raise ValueError(f"Missing SQL parameter: {missing_key}") from exc


def task_execute_sql_file(script_path, parameters=None, database=None, task_name=None):
    task_name = task_name or os.path.basename(script_path)
    sql_script = load_sql_script(script_path)
    if not sql_script:
        return _result(False, task_name, error=f"SQL script not found or empty: {script_path}")

    if parameters:
        try:
            sql_script = _format_sql(sql_script, parameters)
        except ValueError as exc:
            return _result(False, task_name, error=str(exc))

    connection = None
    try:
        connection = connect_to_db(database=database)
        result = execute_sql_script(connection, sql_script)
        return _result(result["success"], task_name, error=result.get("error"))
    except Exception as exc:
        return _result(False, task_name, error=str(exc))
    finally:
        if connection is not None:
            connection.close()


def _clean_excel_dataframe(df):
    """Remove blank Excel columns and normalize column names for SQL inserts."""

    cleaned_columns = []
    columns_to_keep = []

    for column in df.columns:
        if pd.isna(column):
            continue

        column_name = str(column).strip().replace(" ", "")
        if not column_name or column_name.lower() == "nan" or column_name.startswith("Unnamed:"):
            continue

        cleaned_columns.append(column_name)
        columns_to_keep.append(column)

    cleaned_df = df.loc[:, columns_to_keep].copy()
    cleaned_df.columns = cleaned_columns
    return cleaned_df


def _prepare_dataframe_for_sql(df):
    """Convert pandas missing values to Python None for DB-API drivers."""

    return df.astype(object).where(pd.notnull(df), None)


def task_create_dimensional_database():
    return task_execute_sql_file(
        os.path.join(INFRASTRUCTURE_DIR, "dimensional_database_creation.sql"),
        database="master",
        task_name="create_dimensional_database",
    )


def task_create_staging_raw_tables():
    return task_execute_sql_file(
        os.path.join(INFRASTRUCTURE_DIR, "staging_raw_table_creation.sql"),
        task_name="create_staging_raw_tables",
    )


def task_create_dimensional_tables():
    return task_execute_sql_file(
        os.path.join(INFRASTRUCTURE_DIR, "dimensional_db_table_creation.sql"),
        task_name="create_dimensional_tables",
    )


def task_ingest_excel_sheet(file_path, sheet_name, table_name=None):
    """Load one Excel sheet into its matching staging_raw table."""

    table_name = table_name or f"staging_raw_{sheet_name}"
    if not table_name.startswith("staging_raw_"):
        table_name = f"staging_raw_{table_name}"

    connection = None
    cursor = None

    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        df = _clean_excel_dataframe(df)
        if df.empty and len(df.columns) == 0:
            return _result(False, f"ingest_{sheet_name}", error="No valid columns found", table=table_name)

        df = _prepare_dataframe_for_sql(df)

        columns = ", ".join(f"[{column}]" for column in df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))

        connection = connect_to_db()
        cursor = connection.cursor()
        cursor.execute(f"TRUNCATE TABLE [{SCHEMA_NAME}].[{table_name}]")

        insert_sql = f"INSERT INTO [{SCHEMA_NAME}].[{table_name}] ({columns}) VALUES ({placeholders})"
        cursor.executemany(insert_sql, df.values.tolist())
        connection.commit()

        return _result(True, f"ingest_{sheet_name}", rows=len(df), table=table_name)
    except Exception as exc:
        if connection is not None:
            connection.rollback()
        return _result(False, f"ingest_{sheet_name}", error=str(exc), table=table_name)
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def task_ingest_all_staging_raw_tables(file_path):
    for sheet_name in STAGING_SHEETS:
        result = task_ingest_excel_sheet(file_path, sheet_name)
        if not result["success"]:
            return result

    return _result(True, "ingest_all_staging_raw_tables")


def task_update_dimension(dimension_config):
    cfg = parse_db_config()
    if cfg is None:
        return _result(False, f"update_dim_{dimension_config['name']}", error="Database config not found")

    parameters = {
        "database_name": cfg["database"],
        "schema_name": SCHEMA_NAME,
        "source_table_name": dimension_config["source_table_name"],
        "target_table_name": dimension_config["target_table_name"],
    }

    return task_execute_sql_file(
        os.path.join(QUERIES_DIR, dimension_config["query"]),
        parameters=parameters,
        task_name=f"update_dim_{dimension_config['name']}",
    )


def task_update_all_dimensions():
    for dimension_config in DIMENSION_LOAD_ORDER:
        result = task_update_dimension(dimension_config)
        if not result["success"]:
            return result

    return _result(True, "update_all_dimensions")


def task_update_fact(start_date, end_date):
    cfg = parse_db_config()
    if cfg is None:
        return _result(False, "update_fact", error="Database config not found")

    parameters = {
        "database_name": cfg["database"],
        "schema_name": SCHEMA_NAME,
        "source_orders_table_name": "staging_raw_Orders",
        "source_order_details_table_name": "staging_raw_OrderDetails",
        "target_table_name": "FactOrders",
        "start_date": start_date,
        "end_date": end_date,
    }

    return task_execute_sql_file(
        os.path.join(QUERIES_DIR, "update_fact.sql"),
        parameters=parameters,
        task_name="update_fact",
    )


def task_update_fact_error(start_date, end_date):
    cfg = parse_db_config()
    if cfg is None:
        return _result(False, "update_fact_error", error="Database config not found")

    parameters = {
        "database_name": cfg["database"],
        "schema_name": SCHEMA_NAME,
        "source_orders_table_name": "staging_raw_Orders",
        "source_order_details_table_name": "staging_raw_OrderDetails",
        "target_table_name": "FactOrders_Error",
        "start_date": start_date,
        "end_date": end_date,
    }

    return task_execute_sql_file(
        os.path.join(QUERIES_DIR, "update_fact_error.sql"),
        parameters=parameters,
        task_name="update_fact_error",
    )
