from unittest.mock import MagicMock, mock_open, patch

import pandas as pd

import utils


def test_generate_execution_id_success():
    result = utils.generate_execution_id()

    assert isinstance(result, str)
    assert len(result) == 36


def test_execution_id_uniqueness():
    id1 = utils.generate_execution_id()
    id2 = utils.generate_execution_id()

    assert id1 != id2


def test_load_sql_script_success():
    with patch("builtins.open", mock_open(read_data="SELECT 1;")) as mocked_open:
        result = utils.load_sql_script("query.sql")

    assert result == "SELECT 1;"
    mocked_open.assert_called_once_with("query.sql", "r", encoding="utf-8")


def test_load_sql_script_missing_file_returns_none():
    with patch("builtins.open", side_effect=FileNotFoundError):
        result = utils.load_sql_script("missing.sql")

    assert result is None


def test_load_sql_script_missing_path_returns_none():
    assert utils.load_sql_script(None) is None


def test_parse_db_config_success(tmp_path):
    config_file = tmp_path / "sql_server_config.cfg"
    config_file.write_text(
        """
[sql_server]
driver = ODBC Driver 18 for SQL Server
server = localhost
port = 1433
database = ORDER_DDS
trusted_connection = yes
encrypt = yes
trust_server_certificate = yes
user = sa
password = Password123!
""".strip(),
        encoding="utf-8",
    )

    result = utils.parse_db_config(str(config_file), env_path=str(tmp_path / ".env"))

    assert result == {
        "driver": "ODBC Driver 18 for SQL Server",
        "server": "localhost",
        "port": 1433,
        "database": "ORDER_DDS",
        "trusted_connection": True,
        "encrypt": True,
        "trust_server_certificate": True,
        "user": "sa",
        "password": "Password123!",
    }


def test_parse_db_config_env_password_override(tmp_path):
    config_file = tmp_path / "sql_server_config.cfg"
    env_file = tmp_path / ".env"
    config_file.write_text(
        """
[sql_server]
server = localhost
port = 1433
database = ORDER_DDS
user = sa
password =
""".strip(),
        encoding="utf-8",
    )
    env_file.write_text("MSSQL_PASSWORD=SecretFromEnv\n", encoding="utf-8")

    result = utils.parse_db_config(str(config_file), env_path=str(env_file))

    assert result["user"] == "sa"
    assert result["password"] == "SecretFromEnv"


def test_parse_db_config_handles_loose_key_spacing_and_named_instance(tmp_path):
    config_file = tmp_path / "sql_server_config.cfg"
    config_file.write_text(
        """
[sql_server]
driver = ODBC Driver 18 for SQL Server
server = localhost/SQLEXPRESS
database = ORDER_DDS
trusted _connection = yes
encrypt = yes
trust_ server_certificate = yes
""".strip(),
        encoding="utf-8",
    )

    result = utils.parse_db_config(str(config_file), env_path=str(tmp_path / ".env"))

    assert result["server"] == "localhost\\SQLEXPRESS"
    assert result["port"] == 1433
    assert result["database"] == "ORDER_DDS"
    assert result["trusted_connection"] is True
    assert result["encrypt"] is True
    assert result["trust_server_certificate"] is True


def test_parse_db_config_env_only_overrides_password(tmp_path):
    config_file = tmp_path / "sql_server_config.cfg"
    env_file = tmp_path / ".env"
    config_file.write_text(
        """
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
""".strip(),
        encoding="utf-8",
    )
    env_file.write_text(
        """
MSSQL_SERVER=127.0.0.1
MSSQL_PORT=11433
MSSQL_DATABASE=OTHER_DDS
MSSQL_USER=app_user
MSSQL_PASSWORD=SecretFromEnv
MSSQL_TRUSTED_CONNECTION=yes
MSSQL_ENCRYPT=no
MSSQL_TRUST_SERVER_CERTIFICATE=no
""".strip(),
        encoding="utf-8",
    )

    result = utils.parse_db_config(str(config_file), env_path=str(env_file))

    assert result["server"] == "localhost"
    assert result["port"] == 1433
    assert result["database"] == "ORDER_DDS"
    assert result["user"] == "sa"
    assert result["password"] == "SecretFromEnv"
    assert result["trusted_connection"] is False
    assert result["encrypt"] is True
    assert result["trust_server_certificate"] is True


def test_parse_db_config_legacy_sa_password_env_key(tmp_path):
    config_file = tmp_path / "sql_server_config.cfg"
    env_file = tmp_path / ".env"
    config_file.write_text(
        """
[sql_server]
server = localhost
port = 1433
database = ORDER_DDS
user = sa
password =
""".strip(),
        encoding="utf-8",
    )
    env_file.write_text("MSSQL_SA_PASSWORD=SecretFromLegacyEnv\n", encoding="utf-8")

    result = utils.parse_db_config(str(config_file), env_path=str(env_file))

    assert result["user"] == "sa"
    assert result["password"] == "SecretFromLegacyEnv"


def test_build_odbc_connection_string_trusted_connection():
    cfg = {
        "driver": "ODBC Driver 18 for SQL Server",
        "server": "localhost\\SQLEXPRESS",
        "port": 1433,
        "database": "ORDER_DDS",
        "trusted_connection": True,
        "encrypt": True,
        "trust_server_certificate": True,
        "user": "",
        "password": "",
    }

    result = utils.build_odbc_connection_string(cfg)

    assert result == (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost\\SQLEXPRESS;"
        "DATABASE=ORDER_DDS;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Trusted_Connection=yes"
    )


def test_build_odbc_connection_string_sql_auth_with_non_default_port():
    cfg = {
        "driver": "ODBC Driver 18 for SQL Server",
        "server": "127.0.0.1",
        "port": 11433,
        "database": "ORDER_DDS",
        "trusted_connection": False,
        "encrypt": False,
        "trust_server_certificate": True,
        "user": "sa",
        "password": "Password123!",
    }

    result = utils.build_odbc_connection_string(cfg, database="master")

    assert result == (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=127.0.0.1,11433;"
        "DATABASE=master;"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
        "UID=sa;"
        "PWD=Password123!"
    )


def test_connect_to_db_uses_pyodbc_for_trusted_connection():
    cfg = {
        "driver": "ODBC Driver 18 for SQL Server",
        "server": "localhost\\SQLEXPRESS",
        "port": 1433,
        "database": "ORDER_DDS",
        "trusted_connection": True,
        "encrypt": True,
        "trust_server_certificate": True,
        "user": "",
        "password": "",
    }

    with patch("utils.parse_db_config", return_value=cfg), patch("utils.pyodbc.connect") as connect:
        utils.connect_to_db()

    connect.assert_called_once_with(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost\\SQLEXPRESS;"
        "DATABASE=ORDER_DDS;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Trusted_Connection=yes"
    )


def test_connect_to_db_uses_pymssql_without_odbc_driver_or_trusted_connection():
    cfg = {
        "driver": "",
        "server": "localhost",
        "port": 1433,
        "database": "ORDER_DDS",
        "trusted_connection": False,
        "encrypt": True,
        "trust_server_certificate": True,
        "user": "sa",
        "password": "Password123!",
    }

    with patch("utils.parse_db_config", return_value=cfg), patch("utils.pymssql.connect") as connect:
        utils.connect_to_db(database="master")

    connect.assert_called_once_with(
        server="localhost",
        user="sa",
        password="Password123!",
        database="master",
        port=1433,
    )


def test_connect_to_db_prefers_pymssql_for_sql_auth_even_when_odbc_driver_is_configured():
    cfg = {
        "driver": "ODBC Driver 18 for SQL Server",
        "server": "localhost",
        "port": 1433,
        "database": "ORDER_DDS",
        "trusted_connection": False,
        "encrypt": True,
        "trust_server_certificate": True,
        "user": "sa",
        "password": "Password123!",
    }

    with patch("utils.parse_db_config", return_value=cfg), patch("utils.pymssql.connect") as pymssql_connect, patch(
        "utils.pyodbc.connect"
    ) as pyodbc_connect:
        utils.connect_to_db()

    pymssql_connect.assert_called_once_with(
        server="localhost",
        user="sa",
        password="Password123!",
        database="ORDER_DDS",
        port=1433,
    )
    pyodbc_connect.assert_not_called()


def test_connect_to_db_falls_back_to_pyodbc_for_sql_auth_when_pymssql_fails():
    cfg = {
        "driver": "ODBC Driver 18 for SQL Server",
        "server": "localhost\\SQLEXPRESS",
        "port": 1433,
        "database": "ORDER_DDS",
        "trusted_connection": False,
        "encrypt": False,
        "trust_server_certificate": True,
        "user": "sa",
        "password": "Password123!",
    }

    with patch("utils.parse_db_config", return_value=cfg), patch(
        "utils.pymssql.connect", side_effect=RuntimeError("pymssql failed")
    ), patch("utils.pyodbc.connect") as pyodbc_connect:
        utils.connect_to_db()

    pyodbc_connect.assert_called_once_with(
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost\\SQLEXPRESS;"
        "DATABASE=ORDER_DDS;"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
        "UID=sa;"
        "PWD=Password123!"
    )


def test_get_sql_parameter_placeholder_for_pyodbc_connection():
    PyodbcConnection = type("Connection", (), {})
    PyodbcConnection.__module__ = "pyodbc"

    assert utils.get_sql_parameter_placeholder(PyodbcConnection()) == "?"


def test_get_sql_parameter_placeholder_for_pymssql_connection():
    PymssqlConnection = type("Connection", (), {})
    PymssqlConnection.__module__ = "pymssql"

    assert utils.get_sql_parameter_placeholder(PymssqlConnection()) == "%s"


def test_parse_db_config_missing_file_returns_none(tmp_path):
    result = utils.parse_db_config(str(tmp_path / "missing.cfg"), env_path=str(tmp_path / ".env"))

    assert result is None


def test_parse_db_config_missing_section_returns_none(tmp_path):
    config_file = tmp_path / "empty.cfg"
    config_file.write_text("[other]\nserver = localhost\n", encoding="utf-8")

    result = utils.parse_db_config(str(config_file), env_path=str(tmp_path / ".env"))

    assert result is None


def test_execute_sql_script_success():
    connection = MagicMock()
    cursor = connection.cursor.return_value

    result = utils.execute_sql_script(connection, "SELECT 1;")

    assert result == {"success": True}
    cursor.execute.assert_called_once_with("SELECT 1;")
    connection.commit.assert_called_once()
    connection.rollback.assert_not_called()
    cursor.close.assert_called_once()


def test_execute_sql_script_splits_go_batches():
    connection = MagicMock()
    cursor = connection.cursor.return_value

    result = utils.execute_sql_script(connection, "SELECT 1;\nGO\nSELECT 2;")

    assert result == {"success": True}
    assert cursor.execute.call_count == 2
    cursor.execute.assert_any_call("SELECT 1;")
    cursor.execute.assert_any_call("SELECT 2;")
    connection.commit.assert_called_once()


def test_execute_sql_script_failure_rolls_back():
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.execute.side_effect = RuntimeError("database failed")

    result = utils.execute_sql_script(connection, "SELECT 1;")

    assert result["success"] is False
    assert "database failed" in result["error"]
    connection.commit.assert_not_called()
    connection.rollback.assert_called_once()
    cursor.close.assert_called_once()


def test_execute_sql_script_empty_sql_returns_false():
    connection = MagicMock()

    result = utils.execute_sql_script(connection, "")

    assert result == {"success": False}
    connection.cursor.assert_not_called()


def test_clean_excel_dataframe_drops_blank_columns():
    df = pd.DataFrame(
        {
            "Customer ID": ["ALFKI"],
            float("nan"): ["bad"],
            "Unnamed: 12": ["bad"],
            "": ["bad"],
        }
    )

    result = utils.clean_excel_dataframe(df)

    assert list(result.columns) == ["CustomerID"]
    assert result.to_dict("records") == [{"CustomerID": "ALFKI"}]


def test_prepare_dataframe_for_sql_converts_nan_to_none():
    df = pd.DataFrame({"Region": [float("nan"), "WA"]})

    result = utils.prepare_dataframe_for_sql(df)

    assert result.values.tolist() == [[None], ["WA"]]
