import re
import uuid
from configparser import ConfigParser, Error as ConfigParserError

import pandas as pd
import pymssql
from dotenv import dotenv_values

from pipeline_dimensional_data.config import ENV_PATH, SQL_SERVER_CONFIG_PATH

try:
    import pyodbc
except ImportError:
    pyodbc = None


DEFAULT_SQL_SERVER_CONFIG_PATH = str(SQL_SERVER_CONFIG_PATH)
DEFAULT_ENV_PATH = str(ENV_PATH)


def _read_bool(value, default=False):
    if value is None or value == "":
        return default

    return str(value).strip().lower() in {"yes", "true", "1", "on"}


def _read_int(value, default):
    if value is None or value == "":
        return default

    return int(value)


def _normalize_config_key(key):
    return key.strip().lower().replace(" ", "")


def _normalize_sql_server_name(server):
    if not server:
        return server

    # SQL Server named instances are normally written HOST\\INSTANCE.
    # Some config examples use HOST/INSTANCE; normalize that common typo.
    if "/" in server and "\\" not in server:
        return server.replace("/", "\\")

    return server


def _yes_no(value):
    return "yes" if value else "no"


def build_odbc_connection_string(cfg, database=None):
    server = cfg["server"]
    if cfg["port"] != 1433:
        server = f"{server},{cfg['port']}"

    parts = [
        f"DRIVER={{{cfg['driver']}}}",
        f"SERVER={server}",
        f"DATABASE={database or cfg['database']}",
        f"Encrypt={_yes_no(cfg['encrypt'])}",
        f"TrustServerCertificate={_yes_no(cfg['trust_server_certificate'])}",
    ]

    if cfg["trusted_connection"]:
        parts.append("Trusted_Connection=yes")
    else:
        if cfg.get("user"):
            parts.append(f"UID={cfg['user']}")
        if cfg.get("password"):
            parts.append(f"PWD={cfg['password']}")

    return ";".join(parts)


def get_sql_parameter_placeholder(connection):
    module_name = connection.__class__.__module__.lower()
    if module_name.startswith("pyodbc"):
        return "?"

    return "%s"


def _connect_with_odbc(cfg, database=None):
    if pyodbc is None:
        raise ImportError(
            "pyodbc is required for ODBC Driver or trusted SQL Server connections. "
            "Install it with: pip install pyodbc"
        )

    return pyodbc.connect(build_odbc_connection_string(cfg, database=database))


def _connect_with_pymssql(cfg, database=None):
    return pymssql.connect(
        server=cfg["server"],
        user=cfg.get("user") or None,
        password=cfg.get("password") or None,
        database=database or cfg["database"],
        port=cfg["port"],
    )


def generate_execution_id():
    """Generate a unique UUID string for pipeline execution tracking."""
    return str(uuid.uuid4())


def load_sql_script(file_path):
    """Read and return the contents of an SQL script file."""

    if not file_path:
        return None

    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return file.read()
    except (FileNotFoundError, OSError):
        return None


def parse_db_config(
    config_path=DEFAULT_SQL_SERVER_CONFIG_PATH,
    section="sql_server",
    env_path=DEFAULT_ENV_PATH,
):
    """Parse SQL Server connection settings from a config file."""

    parser = ConfigParser()

    try:
        read_files = parser.read(config_path)
    except ConfigParserError:
        return None

    if not read_files or not parser.has_section(section):
        return None

    cfg = {_normalize_config_key(key): value.strip() for key, value in parser.items(section)}
    env_cfg = dotenv_values(env_path) if env_path else {}

    server = cfg.get("server", "localhost")
    port = _read_int(cfg.get("port"), 1433)
    database = cfg.get("database", "ORDER_DDS")
    user = cfg.get("user", "")
    password = env_cfg.get("MSSQL_PASSWORD") or env_cfg.get("MSSQL_SA_PASSWORD") or cfg.get("password", "")

    return {
        "driver": cfg.get("driver", ""),
        "server": _normalize_sql_server_name(server),
        "port": port,
        "database": database,
        "trusted_connection": _read_bool(
            cfg.get("trusted_connection"),
            default=False,
        ),
        "encrypt": _read_bool(cfg.get("encrypt"), default=True),
        "trust_server_certificate": _read_bool(
            cfg.get("trust_server_certificate"),
            default=True,
        ),
        "user": user,
        "password": password,
    }


def connect_to_db(database=None):
    cfg = parse_db_config()
    if cfg is None:
        raise ValueError("SQL Server configuration could not be loaded.")

    if cfg["trusted_connection"]:
        return _connect_with_odbc(cfg, database=database)

    try:
        return _connect_with_pymssql(cfg, database=database)
    except Exception as pymssql_exc:
        if not cfg.get("driver") or pyodbc is None:
            raise

        try:
            return _connect_with_odbc(cfg, database=database)
        except Exception as odbc_exc:
            raise RuntimeError(
                "Could not connect to SQL Server with pymssql or pyodbc. "
                f"pymssql error: {pymssql_exc}; pyodbc error: {odbc_exc}"
            ) from odbc_exc


def format_sql(sql_script, parameters):
    try:
        return sql_script.format(**parameters)
    except KeyError as exc:
        missing_key = exc.args[0]
        raise ValueError(f"Missing SQL parameter: {missing_key}") from exc


def clean_excel_dataframe(df):
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


def prepare_dataframe_for_sql(df):
    """Convert pandas missing values to Python None for DB-API drivers."""

    return df.astype(object).where(pd.notnull(df), None)


def execute_sql_script(connection, sql_script):
    """Execute an SQL script through a DB-API compatible connection."""

    if connection is None or not sql_script:
        return {"success": False}

    batches = [
        batch.strip()
        for batch in re.split(r"(?im)^\s*GO\s*;?\s*$", sql_script)
        if batch.strip()
    ]

    cursor = None
    try:
        cursor = connection.cursor()
        for batch in batches:
            cursor.execute(batch)
        connection.commit()
        return {"success": True}
    except Exception as exc:
        try:
            connection.rollback()
        except Exception:
            pass
        return {"success": False, "error": str(exc)}
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
