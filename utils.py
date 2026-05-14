import os
import re
import uuid
from configparser import ConfigParser, Error as ConfigParserError

from dotenv import dotenv_values


PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SQL_SERVER_CONFIG_PATH = os.path.join(
    PROJECT_ROOT,
    "infrastructure_initiation",
    "sql_server_config.cfg",
)
DEFAULT_ENV_PATH = os.path.join(PROJECT_ROOT, ".env")


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
        print(f"Warning: SQL file not found at {file_path}")
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
    except ConfigParserError as exc:
        print(f"Warning: could not parse database config at {config_path}: {exc}")
        return None

    if not read_files or not parser.has_section(section):
        print(f"Warning: database config section [{section}] not found at {config_path}")
        return None

    cfg = {key: value for key, value in parser.items(section)}
    env_cfg = dotenv_values(env_path) if env_path else {}
    env_password = env_cfg.get("MSSQL_PASSWORD") or env_cfg.get("MSSQL_SA_PASSWORD")

    return {
        "driver": cfg.get("driver", ""),
        "server": cfg.get("server", "localhost"),
        "port": int(cfg.get("port", 1433)),
        "database": cfg.get("database", "ORDER_DDS"),
        "trusted_connection": cfg.get("trusted_connection", "no").lower() in {"yes", "true", "1"},
        "encrypt": cfg.get("encrypt", "yes").lower() in {"yes", "true", "1"},
        "trust_server_certificate": cfg.get("trust_server_certificate", "yes").lower() in {"yes", "true", "1"},
        "user": cfg.get("user", ""),
        "password": env_password or cfg.get("password", ""),
    }


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
        print(f"Error executing SQL script: {exc}")
        return {"success": False, "error": str(exc)}
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
