from configparser import Error as ConfigParserError
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd
import pytest

import utils


def _write_config(path, body):
    path.write_text(body.strip(), encoding="utf-8")
    return str(path)


# generate_execution_id
def test_generate_execution_id_returns_string_uuid():
    result = utils.generate_execution_id()

    assert isinstance(result, str)
    assert len(result) == 36


def test_generate_execution_id_is_parseable_uuid():
    import uuid

    result = utils.generate_execution_id()

    assert str(uuid.UUID(result)) == result


def test_generate_execution_id_generates_unique_values():
    assert utils.generate_execution_id() != utils.generate_execution_id()


def test_generate_execution_id_uses_uuid4():
    with patch("utils.uuid.uuid4", return_value="fixed-id"):
        assert utils.generate_execution_id() == "fixed-id"


# _read_bool
@pytest.mark.parametrize("value", ["yes", "true", "1", "on", " YES "])
def test_read_bool_true_values(value):
    assert utils._read_bool(value) is True


@pytest.mark.parametrize("value", ["no", "false", "0", "off", "anything"])
def test_read_bool_false_values(value):
    assert utils._read_bool(value) is False


def test_read_bool_uses_default_for_none():
    assert utils._read_bool(None, default=True) is True


def test_read_bool_uses_default_for_empty_string():
    assert utils._read_bool("", default=True) is True


# _read_int
def test_read_int_parses_integer_string():
    assert utils._read_int("1433", 0) == 1433


def test_read_int_returns_default_for_none():
    assert utils._read_int(None, 1433) == 1433


def test_read_int_returns_default_for_empty_string():
    assert utils._read_int("", 1433) == 1433


def test_read_int_raises_for_invalid_integer():
    with pytest.raises(ValueError):
        utils._read_int("not-a-port", 1433)


# _normalize_config_key
def test_normalize_config_key_strips_and_lowercases():
    assert utils._normalize_config_key(" SERVER ") == "server"


def test_normalize_config_key_removes_spaces():
    assert utils._normalize_config_key("trusted _connection") == "trusted_connection"


def test_normalize_config_key_handles_internal_spaces():
    assert utils._normalize_config_key("trust_ server_certificate") == "trust_server_certificate"


def test_normalize_config_key_keeps_underscores():
    assert utils._normalize_config_key("trust_server_certificate") == "trust_server_certificate"


# _normalize_sql_server_name
def test_normalize_sql_server_name_keeps_plain_host():
    assert utils._normalize_sql_server_name("localhost") == "localhost"


def test_normalize_sql_server_name_converts_slash_instance():
    assert utils._normalize_sql_server_name("localhost/SQLEXPRESS") == "localhost\\SQLEXPRESS"


def test_normalize_sql_server_name_keeps_backslash_instance():
    assert utils._normalize_sql_server_name("localhost\\SQLEXPRESS") == "localhost\\SQLEXPRESS"


def test_normalize_sql_server_name_returns_empty_values_unchanged():
    assert utils._normalize_sql_server_name("") == ""
    assert utils._normalize_sql_server_name(None) is None


# _yes_no
def test_yes_no_true_returns_yes():
    assert utils._yes_no(True) == "yes"


def test_yes_no_false_returns_no():
    assert utils._yes_no(False) == "no"


def test_yes_no_truthy_value_returns_yes():
    assert utils._yes_no(1) == "yes"


def test_yes_no_falsy_value_returns_no():
    assert utils._yes_no(0) == "no"


# build_odbc_connection_string
def _odbc_cfg(**overrides):
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
    cfg.update(overrides)
    return cfg


def test_build_odbc_connection_string_sql_auth_default_port():
    result = utils.build_odbc_connection_string(_odbc_cfg())

    assert "SERVER=localhost" in result
    assert "UID=sa" in result
    assert "PWD=Password123!" in result


def test_build_odbc_connection_string_sql_auth_non_default_port():
    result = utils.build_odbc_connection_string(_odbc_cfg(server="127.0.0.1", port=11433), database="master")

    assert "SERVER=127.0.0.1,11433" in result
    assert "DATABASE=master" in result


def test_build_odbc_connection_string_trusted_connection():
    result = utils.build_odbc_connection_string(_odbc_cfg(trusted_connection=True, user="", password=""))

    assert "Trusted_Connection=yes" in result
    assert "UID=" not in result
    assert "PWD=" not in result


def test_build_odbc_connection_string_encrypt_flags():
    result = utils.build_odbc_connection_string(_odbc_cfg(encrypt=False, trust_server_certificate=False))

    assert "Encrypt=no" in result
    assert "TrustServerCertificate=no" in result


# get_sql_parameter_placeholder
def test_get_sql_parameter_placeholder_for_pyodbc_connection():
    PyodbcConnection = type("Connection", (), {})
    PyodbcConnection.__module__ = "pyodbc"

    assert utils.get_sql_parameter_placeholder(PyodbcConnection()) == "?"


def test_get_sql_parameter_placeholder_for_pymssql_connection():
    PymssqlConnection = type("Connection", (), {})
    PymssqlConnection.__module__ = "pymssql"

    assert utils.get_sql_parameter_placeholder(PymssqlConnection()) == "%s"


def test_get_sql_parameter_placeholder_for_nested_pyodbc_module():
    Conn = type("Connection", (), {})
    Conn.__module__ = "pyodbc.connectors"

    assert utils.get_sql_parameter_placeholder(Conn()) == "?"


def test_get_sql_parameter_placeholder_defaults_to_percent_s():
    Conn = type("Connection", (), {})
    Conn.__module__ = "custom_driver"

    assert utils.get_sql_parameter_placeholder(Conn()) == "%s"


# _connect_with_odbc
def test_connect_with_odbc_calls_pyodbc_connect():
    cfg = _odbc_cfg()
    with patch("utils.pyodbc.connect", return_value="conn") as connect:
        result = utils._connect_with_odbc(cfg)

    assert result == "conn"
    connect.assert_called_once()


def test_connect_with_odbc_uses_database_override():
    cfg = _odbc_cfg()
    with patch("utils.pyodbc.connect", return_value="conn") as connect:
        utils._connect_with_odbc(cfg, database="master")

    assert "DATABASE=master" in connect.call_args.args[0]


def test_connect_with_odbc_raises_if_pyodbc_missing():
    cfg = _odbc_cfg()
    with patch("utils.pyodbc", None):
        with pytest.raises(ImportError):
            utils._connect_with_odbc(cfg)


def test_connect_with_odbc_propagates_connect_error():
    cfg = _odbc_cfg()
    with patch("utils.pyodbc.connect", side_effect=RuntimeError("odbc failed")):
        with pytest.raises(RuntimeError, match="odbc failed"):
            utils._connect_with_odbc(cfg)


# _connect_with_pymssql
def test_connect_with_pymssql_calls_pymssql_connect():
    cfg = _odbc_cfg(driver="")
    with patch("utils.pymssql.connect", return_value="conn") as connect:
        result = utils._connect_with_pymssql(cfg)

    assert result == "conn"
    connect.assert_called_once_with(
        server="localhost",
        user="sa",
        password="Password123!",
        database="ORDER_DDS",
        port=1433,
    )


def test_connect_with_pymssql_uses_database_override():
    cfg = _odbc_cfg(driver="")
    with patch("utils.pymssql.connect", return_value="conn") as connect:
        utils._connect_with_pymssql(cfg, database="master")

    assert connect.call_args.kwargs["database"] == "master"


def test_connect_with_pymssql_converts_empty_credentials_to_none():
    cfg = _odbc_cfg(driver="", user="", password="")
    with patch("utils.pymssql.connect", return_value="conn") as connect:
        utils._connect_with_pymssql(cfg)

    assert connect.call_args.kwargs["user"] is None
    assert connect.call_args.kwargs["password"] is None


def test_connect_with_pymssql_propagates_error():
    cfg = _odbc_cfg(driver="")
    with patch("utils.pymssql.connect", side_effect=RuntimeError("pymssql failed")):
        with pytest.raises(RuntimeError, match="pymssql failed"):
            utils._connect_with_pymssql(cfg)


# load_sql_script
def test_load_sql_script_success():
    with patch("builtins.open", mock_open(read_data="SELECT 1;")) as mocked_open:
        result = utils.load_sql_script("query.sql")

    assert result == "SELECT 1;"
    mocked_open.assert_called_once_with("query.sql", "r", encoding="utf-8")


def test_load_sql_script_missing_file_returns_none():
    with patch("builtins.open", side_effect=FileNotFoundError):
        assert utils.load_sql_script("missing.sql") is None


def test_load_sql_script_os_error_returns_none():
    with patch("builtins.open", side_effect=OSError):
        assert utils.load_sql_script("bad.sql") is None


def test_load_sql_script_missing_path_returns_none():
    assert utils.load_sql_script(None) is None


# parse_db_config
def test_parse_db_config_success(tmp_path):
    config_file = _write_config(
        tmp_path / "sql_server_config.cfg",
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
        """,
    )

    result = utils.parse_db_config(config_file, env_path=str(tmp_path / ".env"))

    assert result["server"] == "localhost"
    assert result["trusted_connection"] is True
    assert result["password"] == "Password123!"


def test_parse_db_config_env_password_override(tmp_path):
    config_file = _write_config(
        tmp_path / "sql_server_config.cfg",
        """
        [sql_server]
        server = localhost
        user = sa
        password =
        """,
    )
    env_file = tmp_path / ".env"
    env_file.write_text("MSSQL_PASSWORD=SecretFromEnv\n", encoding="utf-8")

    result = utils.parse_db_config(config_file, env_path=str(env_file))

    assert result["password"] == "SecretFromEnv"


def test_parse_db_config_handles_loose_key_spacing_and_named_instance(tmp_path):
    config_file = _write_config(
        tmp_path / "sql_server_config.cfg",
        """
        [sql_server]
        server = localhost/SQLEXPRESS
        trusted _connection = yes
        trust_ server_certificate = yes
        """,
    )

    result = utils.parse_db_config(config_file, env_path=str(tmp_path / ".env"))

    assert result["server"] == "localhost\\SQLEXPRESS"
    assert result["trusted_connection"] is True
    assert result["trust_server_certificate"] is True


def test_parse_db_config_missing_file_returns_none(tmp_path):
    assert utils.parse_db_config(str(tmp_path / "missing.cfg"), env_path=str(tmp_path / ".env")) is None


def test_parse_db_config_missing_section_returns_none(tmp_path):
    config_file = _write_config(tmp_path / "empty.cfg", "[other]\nserver = localhost")

    assert utils.parse_db_config(config_file, env_path=str(tmp_path / ".env")) is None


def test_parse_db_config_parser_error_returns_none(tmp_path):
    with patch("utils.ConfigParser.read", side_effect=ConfigParserError):
        assert utils.parse_db_config(str(tmp_path / "bad.cfg"), env_path=str(tmp_path / ".env")) is None


# connect_to_db
def test_connect_to_db_raises_if_config_missing():
    with patch("utils.parse_db_config", return_value=None):
        with pytest.raises(ValueError):
            utils.connect_to_db()


def test_connect_to_db_uses_odbc_for_trusted_connection():
    cfg = _odbc_cfg(trusted_connection=True)
    with patch("utils.parse_db_config", return_value=cfg), patch("utils._connect_with_odbc", return_value="conn") as connect:
        assert utils.connect_to_db() == "conn"

    connect.assert_called_once_with(cfg, database=None)


def test_connect_to_db_prefers_pymssql_for_sql_auth():
    cfg = _odbc_cfg(trusted_connection=False)
    with patch("utils.parse_db_config", return_value=cfg), patch("utils._connect_with_pymssql", return_value="conn") as connect:
        assert utils.connect_to_db(database="master") == "conn"

    connect.assert_called_once_with(cfg, database="master")


def test_connect_to_db_falls_back_to_odbc_when_pymssql_fails():
    cfg = _odbc_cfg(trusted_connection=False)
    with patch("utils.parse_db_config", return_value=cfg), patch(
        "utils._connect_with_pymssql", side_effect=RuntimeError("pymssql failed")
    ), patch("utils._connect_with_odbc", return_value="conn") as odbc_connect:
        assert utils.connect_to_db() == "conn"

    odbc_connect.assert_called_once_with(cfg, database=None)


def test_connect_to_db_raises_combined_error_when_both_drivers_fail():
    cfg = _odbc_cfg(trusted_connection=False)
    with patch("utils.parse_db_config", return_value=cfg), patch(
        "utils._connect_with_pymssql", side_effect=RuntimeError("pymssql failed")
    ), patch("utils._connect_with_odbc", side_effect=RuntimeError("odbc failed")):
        with pytest.raises(RuntimeError, match="Could not connect to SQL Server"):
            utils.connect_to_db()


# format_sql
def test_format_sql_replaces_parameters():
    assert utils.format_sql("SELECT {value}", {"value": 1}) == "SELECT 1"


def test_format_sql_replaces_multiple_parameters():
    result = utils.format_sql("[{database}].[{schema}].[{table}]", {"database": "DB", "schema": "dbo", "table": "T"})

    assert result == "[DB].[dbo].[T]"


def test_format_sql_raises_value_error_for_missing_parameter():
    with pytest.raises(ValueError, match="Missing SQL parameter"):
        utils.format_sql("SELECT {missing}", {})


def test_format_sql_leaves_sql_without_placeholders_unchanged():
    assert utils.format_sql("SELECT 1", {"unused": "x"}) == "SELECT 1"


# clean_excel_dataframe
def test_clean_excel_dataframe_drops_blank_columns():
    df = pd.DataFrame({"Customer ID": ["ALFKI"], float("nan"): ["bad"], "Unnamed: 12": ["bad"], "": ["bad"]})

    result = utils.clean_excel_dataframe(df)

    assert list(result.columns) == ["CustomerID"]
    assert result.to_dict("records") == [{"CustomerID": "ALFKI"}]


def test_clean_excel_dataframe_strips_column_spaces():
    df = pd.DataFrame({" Customer ID ": ["ALFKI"]})

    assert list(utils.clean_excel_dataframe(df).columns) == ["CustomerID"]


def test_clean_excel_dataframe_preserves_valid_columns_and_values():
    df = pd.DataFrame({"OrderID": [1], "ProductID": [2]})

    result = utils.clean_excel_dataframe(df)

    assert result.to_dict("records") == [{"OrderID": 1, "ProductID": 2}]


def test_clean_excel_dataframe_returns_empty_dataframe_when_no_valid_columns():
    df = pd.DataFrame({float("nan"): [1], "Unnamed: 0": [2], "": [3]})

    result = utils.clean_excel_dataframe(df)

    assert list(result.columns) == []


# prepare_dataframe_for_sql
def test_prepare_dataframe_for_sql_converts_nan_to_none():
    df = pd.DataFrame({"Region": [float("nan"), "WA"]})

    assert utils.prepare_dataframe_for_sql(df).values.tolist() == [[None], ["WA"]]


def test_prepare_dataframe_for_sql_converts_none_like_datetime_nat():
    df = pd.DataFrame({"OrderDate": [pd.NaT]})

    assert utils.prepare_dataframe_for_sql(df).values.tolist() == [[None]]


def test_prepare_dataframe_for_sql_preserves_regular_values():
    df = pd.DataFrame({"Quantity": [1], "Name": ["A"]})

    assert utils.prepare_dataframe_for_sql(df).values.tolist() == [[1, "A"]]


def test_prepare_dataframe_for_sql_returns_object_dtype():
    df = pd.DataFrame({"Quantity": [1, None]})

    assert utils.prepare_dataframe_for_sql(df)["Quantity"].dtype == object


# execute_sql_script
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

    assert utils.execute_sql_script(connection, "") == {"success": False}
    connection.cursor.assert_not_called()


def test_execute_sql_script_none_connection_returns_false():
    assert utils.execute_sql_script(None, "SELECT 1") == {"success": False}


def test_execute_sql_script_ignores_rollback_failure():
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.execute.side_effect = RuntimeError("execute failed")
    connection.rollback.side_effect = RuntimeError("rollback failed")

    result = utils.execute_sql_script(connection, "SELECT 1;")

    assert result["success"] is False
    assert "execute failed" in result["error"]
