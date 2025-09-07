"""
Unit tests for database module functions.

These tests use pytest and unittest.mock to simulate Postgres connections,
verify generated SQL statements and parameters, and assert expected behavior.
"""
from unittest.mock import MagicMock, patch

import pytest

from database import (
    STATUS_VALUES, _check_table_data_exists, _get_id,
    _get_row_count, _get_new_status, create_table,
    delete_rows, drop_table, insert_rows, update_rows
)


@pytest.fixture
def mock_cursor() -> MagicMock:
    """
    Returns a cursor mock object.

    :returns:
    MagicMock - Cursor mock object.
    """
    mock_cur = MagicMock(name="mock_cur")

    return mock_cur


@pytest.fixture
def mock_connection(mock_cursor: MagicMock) -> MagicMock:
    """
    Returns a mocked Postgres connections object.

    :params:
    mock_cursor (MagicMock) - A mocked Postgres cursor

    :returns:
    MagicMock - A mocked Postgres connection
    """
    mock_cm = MagicMock(name="mock_cm")
    mock_cm.__enter__.return_value = mock_cursor
    mock_cm.__exit__.return_value = False

    mock_conn = MagicMock(name="mock_conn")
    mock_conn.cursor.return_value = mock_cm

    return mock_conn


@pytest.mark.parametrize(
    "fetchone_result, expected",
    [
        (("id-123",), True),
        (None, False),
    ],
)
def test_check_table_data_exists_returns_boolean(
    fetchone_result: tuple[str],
    expected: bool,
    mock_connection: MagicMock,
    mock_cursor: MagicMock
) -> None:
    """
    Tests the return value of the _check_table_data_exists function.

    :params:
    fetchone_result (tuple[str]) - Mocked ID value.
    expected (bool) - The expected return value of the function.
    mock_connection (MagicMock) - A mocked Postgres connection.
    mock_cursor (MagicMock) - A mocked Postgres cursor.
    """
    mock_cursor.fetchone.return_value = fetchone_result

    result = _check_table_data_exists(mock_connection)

    assert result is expected
    mock_cursor.execute.assert_called_once()


@pytest.mark.parametrize(
    "fetchall_results, choice_return, expected",
    [
        ([("id-1",),("id-2",), ("id-3")], "id-2", "id-2"),
        ([("id-1",)], "id-1", "id-1"),
    ]
)
def test_get_id_returns_random_id(
    fetchall_results: list[tuple[str]],
    choice_return: str,
    expected: str,
    mock_connection: MagicMock,
    mock_cursor: MagicMock
) -> None:
    """
    Tests the return value of the _get_id function.

    :params:
    fetchall_results (list[tuple[str]]) - Mocked set of ID values.
    choice_return (str) - A randomly chosen mocked ID value.
    expected (str) - The expected value of the ID.
    mock_connection (MagicMock) - A mocked Postgres connection.
    mock_cursor (MagicMock) - A mocked Postgres cursor.
    """
    mock_cursor.fetchall.return_value = fetchall_results

    with patch("database.random.choice", return_value=choice_return) as mock_random:
        result = _get_id(mock_connection)

    assert result == expected
    mock_cursor.execute.assert_called_once()
    mock_random.assert_called_once()


def test_get_id_raises_runtime_error_if_no_rows(
    mock_connection: MagicMock,
    mock_cursor: MagicMock
) -> None:
    """
    Tests the RuntimeError exception in the _get_id function when there
    are no rows in the orders table.

    :params:
    mock_connection (MagicMock) - A mocked Postgres connection.
    mock_cursor (MagicMock) - A mocked Postgres cursor.
    """
    mock_cursor.fetchall.return_value = []

    with pytest.raises(RuntimeError, match="contains no data"):
        _get_id(mock_connection)

    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchall.assert_called_once()


@pytest.mark.parametrize(
    "fetchone_result, unexpected",
    [
        (("pending",), "pending"),
        (("paid",), "paid"),
        (("shipped",), "shipped"),
        (("cancelled",), "cancelled"),
        (("refunded",), "refunded"),
    ]
)
def test_get_new_status_returns_random_status(
    fetchone_result: tuple[str],
    unexpected: str,
    mock_connection: MagicMock,
    mock_cursor: MagicMock
) -> None:
    """
    Tests the random status return value of the _get_new_status function.

    :params:
    fetchone_result (tuple[str]) - Mocked status value.
    unexpected (str) - The status value that we do not expect the function to return.
    mock_connection (MagicMock) - A mocked Postgres connection.
    mock_cursor (MagicMock) - A mocked Postgres cursor.
    """
    mock_cursor.fetchone.return_value = fetchone_result

    result = _get_new_status(mock_connection, "id-123")

    assert result in STATUS_VALUES
    assert result != unexpected
    mock_cursor.execute.assert_called_once()


def test_get_new_status_params(
    mock_connection: MagicMock,
    mock_cursor: MagicMock
) -> None:
    """
    Tests the SQL statement and parameters executed by the
    _get_new_status function.

    :params:
    mock_connection (MagicMock) - A mocked Postgres connection.
    mock_cursor (MagicMock) - A mocked Postgres cursor.
    """
    _get_new_status(mock_connection, "id-123")

    sql, params = mock_cursor.execute.call_args.args

    expected_sql = """
        SELECT status
        FROM orders
        WHERE id = %s;
    """

    assert " ".join(sql.split()) == " ".join(expected_sql.split())
    assert params == ("id-123",)


def test_get_new_status_raises_runtime_error_if_no_rows(
    mock_connection: MagicMock,
    mock_cursor: MagicMock
) -> None:
    """
    Tests the RuntimeError exception in the _get_new_status function
    when there are no rows in the orders table.

    :params:
    mock_connection (MagicMock) - A mocked Postgres connection.
    mock_cursor (MagicMock) - A mocked Postgres cursor.
    """
    mock_cursor.fetchone.return_value = None

    with pytest.raises(RuntimeError, match="Could not fetch existing status"):
        _get_new_status(mock_connection, "id-123")
    
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchone.assert_called_once()


def test_get_row_count_params(
    mock_connection: MagicMock,
    mock_cursor: MagicMock
) -> None:
    """
    Tests the SQL statement and parameters executed by the
    _get_row_count function.

    :params:
    mock_connection (MagicMock) - A mocked Postgres connection.
    mock_cursor (MagicMock) - A mocked Postgres cursor.
    """
    _get_row_count(mock_connection)

    sql = mock_cursor.execute.call_args.args[0]

    expected_sql = """
        SELECT count(*)
        FROM orders;
    """

    assert " ".join(sql.split()) == " ".join(expected_sql.split())
    mock_cursor.execute.assert_called_once()


def test_get_row_count_raises_runtime_error_if_no_rows(
    mock_connection: MagicMock,
    mock_cursor: MagicMock
) -> None:
    """
    Tests the RuntimeError exception in the _get_row_count function
    when there are no rows in the orders table.

    :params:
    mock_connection (MagicMock) - A mocked Postgres connection.
    mock_cursor (MagicMock) - A mocked Postgres cursor.
    """
    mock_cursor.fetchone.return_value = None
    
    with pytest.raises(RuntimeError, match="Error when querying the number of rows"):
        _get_row_count(mock_connection)
    
    mock_cursor.execute.assert_called_once()
    mock_cursor.fetchone.assert_called_once()


def test_create_table_params(
    mock_connection: MagicMock,
    mock_cursor: MagicMock
) -> None:
    """
    Tests the SQL statement and parameters executed by the
    create_table function.

    :params:
    mock_connection (MagicMock) - A mocked Postgres connection.
    mock_cursor (MagicMock) - A mocked Postgres cursor.
    """
    create_table(mock_connection)
    
    sql = mock_cursor.execute.call_args.args[0]

    expected_sql = """
        CREATE TABLE IF NOT EXISTS orders (
            id                 TEXT    PRIMARY KEY,
            status             TEXT    NOT NULL,
            total_amount_cents INTEGER NOT NULL,
            created_at         BIGINT  NOT NULL,
            last_updated_at    BIGINT  NOT NULL
        );
    """

    assert " ".join(sql.split()) == " ".join(expected_sql.split())
    mock_cursor.execute.assert_called_once()


def test_drop_table_params(
    mock_connection: MagicMock,
    mock_cursor: MagicMock
) -> None:
    """
    Tests the SQL statement and parameters executed by the
    drop_table function.

    :params:
    mock_connection (MagicMock) - A mocked Postgres connection.
    mock_cursor (MagicMock) - A mocked Postgres cursor.
    """
    drop_table(mock_connection)

    sql = mock_cursor.execute.call_args.args[0]

    expected_sql = """
        DROP TABLE IF EXISTS orders;
    """

    assert " ".join(sql.split()) == " ".join(expected_sql.split())
    mock_cursor.execute.assert_called_once()


def test_insert_rows_params(
    mock_connection: MagicMock,
    mock_cursor: MagicMock
) -> None:
    """
    Tests the SQL statement and parameters executed by the
    insert_rows function.

    :params:
    mock_connection (MagicMock) - A mocked Postgres connection.
    mock_cursor (MagicMock) - A mocked Postgres cursor.
    """
    with patch("database._generate_uuid", return_value="id-123"), \
        patch("database._generate_status", return_value="paid"), \
        patch("database._generate_amount", return_value=5000), \
        patch("database._generate_timestamp", return_value=11111):

        insert_rows(mock_connection, n=1)
    
    sql, params = mock_cursor.execute.call_args.args

    expected_sql = """
        INSERT INTO orders
        VALUES (%s, %s, %s, %s, %s);
    """

    assert " ".join(sql.split()) == " ".join(expected_sql.split())
    assert params == ("id-123", "paid", 5000, 11111, 11111)
    mock_cursor.execute.assert_called_once()


def test_update_rows_params(
    mock_connection: MagicMock,
    mock_cursor: MagicMock
) -> None:
    """
    Tests the SQL statement and parameters executed by the
    update_rows function.

    :params:
    mock_connection (MagicMock) - A mocked Postgres connection.
    mock_cursor (MagicMock) - A mocked Postgres cursor.
    """
    with patch("database._check_table_data_exists", return_value=True), \
        patch("database._get_id", return_value="id-123"), \
        patch("database._get_new_status", return_value="new_status"), \
        patch("database._generate_timestamp", return_value=11111):

        update_rows(mock_connection, 1)

    sql, params = mock_cursor.execute.call_args.args

    expected_sql = """
        UPDATE orders
        SET status = %s, last_updated_at = %s
        WHERE id = %s;
    """

    assert " ".join(sql.split()) == " ".join(expected_sql.split())
    assert params == ("new_status", 11111, "id-123")
    mock_cursor.execute.assert_called_once()


def test_update_rows_raise_runtime_error_if_no_rows(mock_connection: MagicMock) -> None:
    """
    Tests the RuntimeError exception in the update_rows function
    when there are no rows in the orders table.

    :params:
    mock_connection (MagicMock) - A mocked Postgres connection.
    """
    with pytest.raises(RuntimeError, match="Orders table does not have any rows"), \
        patch("database._check_table_data_exists", return_value=False) as mock_check:
        
        update_rows(mock_connection, 5)
    
    mock_check.assert_called_once()


def test_delete_rows_params(
    mock_connection: MagicMock,
    mock_cursor: MagicMock
) -> None:
    """
    Tests the SQL statement and parameters executed by the
    delete_rows function.

    :params:
    mock_connection (MagicMock) - A mocked Postgres connection.
    mock_cursor (MagicMock) - A mocked Postgres cursor.
    """
    with patch("database._check_table_data_exists", return_value=True), \
        patch("database._get_row_count", return_value=1), \
        patch("database._get_id", return_value="id-123"):

        delete_rows(mock_connection, 1)

    sql, params = mock_cursor.execute.call_args.args

    expected_sql = """
        DELETE FROM orders
        WHERE id = %s;
    """

    assert " ".join(sql.split()) == " ".join(expected_sql.split())
    assert params == ("id-123",)
    mock_cursor.execute.assert_called_once()


def test_delete_rows_raise_runtime_error_if_no_rows(mock_connection: MagicMock) -> None:
    """
    Tests the RuntimeError exception in the delete_rows function
    when there are no rows in the orders table.

    :params:
    mock_connection (MagicMock) - A mocked Postgres connection.
    """
    with pytest.raises(RuntimeError, match="Orders table does not have any rows"), \
        patch("database._check_table_data_exists", return_value=False) as mock_check:

        delete_rows(mock_connection, 10)

    mock_check.assert_called_once()
