import os

import pandas as pd
from utils import (
    clean_excel_dataframe,
    connect_to_db,
    execute_sql_script,
    format_sql,
    get_sql_parameter_placeholder,
    load_sql_script,
    parse_db_config,
    prepare_dataframe_for_sql,
)

try:
    from .config import (
        DIMENSION_LOAD_ORDER,
        FACT_CONFIG,
        FACT_ERROR_CONFIG,
        QUERIES_DIR,
        SCHEMA_NAME,
        SOURCE_SHEETS,
        SOURCE_TABLES,
    )
except ImportError:
    from config import (
        DIMENSION_LOAD_ORDER,
        FACT_CONFIG,
        FACT_ERROR_CONFIG,
        QUERIES_DIR,
        SCHEMA_NAME,
        SOURCE_SHEETS,
        SOURCE_TABLES,
    )


def _result(success, task_name, **extra):
    result = {"success": success, "task": task_name}
    result.update(extra)
    return result


def task_execute_sql_file(script_path, parameters=None, task_name=None):
    task_name = task_name or os.path.basename(script_path)
    sql_script = load_sql_script(script_path)
    if not sql_script:
        return _result(False, task_name, error=f"SQL script not found or empty: {script_path}")

    if parameters:
        try:
            sql_script = format_sql(sql_script, parameters)
        except ValueError as exc:
            return _result(False, task_name, error=str(exc))

    connection = None
    try:
        connection = connect_to_db()
        result = execute_sql_script(connection, sql_script)
        return _result(
            result["success"],
            task_name,
            error=result.get("error"),
            script=str(script_path),
        )
    except Exception as exc:
        return _result(False, task_name, error=str(exc))
    finally:
        if connection is not None:
            connection.close()


def task_ingest_excel_sheet(file_path, sheet_name, table_name=None):
    """Load one Excel sheet into its matching staging_raw table."""

    table_name = table_name or SOURCE_TABLES[sheet_name]

    connection = None
    cursor = None

    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        df = clean_excel_dataframe(df)
        if df.empty and len(df.columns) == 0:
            return _result(False, f"ingest_{sheet_name}", error="No valid columns found", table=table_name)

        df = prepare_dataframe_for_sql(df)

        connection = connect_to_db()
        cursor = connection.cursor()
        placeholder = get_sql_parameter_placeholder(connection)
        columns = ", ".join(f"[{column}]" for column in df.columns)
        placeholders = ", ".join([placeholder] * len(df.columns))

        if hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True

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


def task_ingest_all_source_tables(file_path):
    table_counts = []

    for sheet_name in SOURCE_SHEETS:
        result = task_ingest_excel_sheet(file_path, sheet_name)
        if not result["success"]:
            return result

        table_counts.append(
            {
                "sheet": sheet_name,
                "table": result["table"],
                "rows": result["rows"],
            }
        )

    return _result(True, "ingest_all_source_tables", table_counts=table_counts)


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

    result = task_execute_sql_file(
        QUERIES_DIR / dimension_config["query"],
        parameters=parameters,
        task_name=f"update_dim_{dimension_config['name']}",
    )
    result.update(
        {
            "source_table": dimension_config["source_table_name"],
            "target_table": dimension_config["target_table_name"],
            "query": dimension_config["query"],
        }
    )
    return result


def task_update_all_dimensions():
    dimensions = []

    for dimension_config in DIMENSION_LOAD_ORDER:
        result = task_update_dimension(dimension_config)
        if not result["success"]:
            return result

        dimensions.append(
            {
                "source_table": result["source_table"],
                "target_table": result["target_table"],
                "query": result["query"],
            }
        )

    return _result(True, "update_all_dimensions", dimensions=dimensions)


def task_update_fact(start_date, end_date):
    cfg = parse_db_config()
    if cfg is None:
        return _result(False, "update_fact", error="Database config not found")

    parameters = {
        "database_name": cfg["database"],
        "schema_name": SCHEMA_NAME,
        "source_orders_table_name": FACT_CONFIG["source_orders_table_name"],
        "source_order_details_table_name": FACT_CONFIG["source_order_details_table_name"],
        "target_table_name": FACT_CONFIG["target_table_name"],
        "start_date": start_date,
        "end_date": end_date,
    }

    result = task_execute_sql_file(
        QUERIES_DIR / FACT_CONFIG["query"],
        parameters=parameters,
        task_name="update_fact",
    )
    result.update(
        {
            "source_orders_table": FACT_CONFIG["source_orders_table_name"],
            "source_order_details_table": FACT_CONFIG["source_order_details_table_name"],
            "target_table": FACT_CONFIG["target_table_name"],
            "query": FACT_CONFIG["query"],
            "start_date": start_date,
            "end_date": end_date,
        }
    )
    return result


def task_update_fact_error(start_date, end_date):
    cfg = parse_db_config()
    if cfg is None:
        return _result(False, "update_fact_error", error="Database config not found")

    parameters = {
        "database_name": cfg["database"],
        "schema_name": SCHEMA_NAME,
        "source_orders_table_name": FACT_ERROR_CONFIG["source_orders_table_name"],
        "source_order_details_table_name": FACT_ERROR_CONFIG["source_order_details_table_name"],
        "target_table_name": FACT_ERROR_CONFIG["target_table_name"],
        "start_date": start_date,
        "end_date": end_date,
    }

    result = task_execute_sql_file(
        QUERIES_DIR / FACT_ERROR_CONFIG["query"],
        parameters=parameters,
        task_name="update_fact_error",
    )
    result.update(
        {
            "source_orders_table": FACT_ERROR_CONFIG["source_orders_table_name"],
            "source_order_details_table": FACT_ERROR_CONFIG["source_order_details_table_name"],
            "target_table": FACT_ERROR_CONFIG["target_table_name"],
            "query": FACT_ERROR_CONFIG["query"],
            "start_date": start_date,
            "end_date": end_date,
        }
    )
    return result
