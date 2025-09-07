"""
Utility module for managing a Postgres `orders` table.

Provides helper functions to generate fake order data, check table state,
and perform CRUD operations (create, insert, update, delete) using psycopg2.
Includes context-managed connection handling and structured logging.
"""
import random
import time
import uuid
from contextlib import contextmanager
from typing import Generator

import psycopg2
from faker import Faker
from psycopg2.errors import UndefinedTable  # type: ignore[attr-defined]
from psycopg2.extensions import connection as PGConnection

from loggers import logger


fake = Faker()

STATUS_VALUES = ["pending", "paid", "shipped", "cancelled", "refunded"]


def _generate_uuid() -> str:
    """
    Generate and return a UUID.

    :returns:
    str - Returns a unique identifier.
    """
    return str(uuid.uuid4())


def _generate_status() -> str:
    """
    Generate and return a random status.

    :returns:
    str - A random status ('pending', 'paid', 'shipping', 'cancelled',
        'refunded')
    """
    status = random.choice(STATUS_VALUES)

    return status


def _generate_amount() -> int:
    """
    Generate and return an integer representing a dollar
    amount in cents.

    :returns:
    int - An interger representing a dollar amount in cents.
    """
    return random.randint(1000, 10000)


def _generate_timestamp() -> int:
    """
    Generate and return an epoch milliseconds timestamp.

    :returns:
    int - Epoch milliseconds timestamp.
    """
    return int(time.time() * 1000)


def _check_table_data_exists(conn: PGConnection) -> bool:
    """
    Checks if the orders table has at least one row.

    :params:
    conn (PGConnection) - Postgres connection object.

    :returns:
    bool - Whether the orders table has at least one row.
    """
    select_stmt = """
        SELECT id
        FROM orders
        LIMIT 1;
    """
    logger.debug(f"Checking if the orders table has at least one row with query: {select_stmt}")

    with conn.cursor() as cur:
        try:
            cur.execute(select_stmt)
            results = cur.fetchone()
        except UndefinedTable as err:
            logger.error(f"Orders table not found.\nPostgres error: {err}")
            raise

    if not results:
        return False
    
    return True


def _get_id(conn: PGConnection) -> str:
    """
    Returns a random id value from the orders table.

    :params:
    conn (PGConnection) - Postgres connection object.

    :returns:
    str - A random id from the orders table. 
    """
    select_stmt = """
        SELECT id
        FROM orders;
    """
    logger.debug(f"Getting random id from the orders table with query: {select_stmt}")

    with conn.cursor() as cur:
        try:
            cur.execute(select_stmt)
            results = cur.fetchall()
        except UndefinedTable as err:
            logger.error(f"Orders table not found.\nPostgres error: {err}")
            raise

    if not results:
        raise RuntimeError("The orders table contains no data.")

    ids = [row[0] for row in results]
    
    return random.choice(ids)


def _get_new_status(conn: PGConnection, id: str) -> str:
    """
    Returns a new status that is different from the current one
    for an existing row in the orders table.

    :params:
    conn (PGConnection) - Postgres connection object.
    id (str) - An id from the orders table that will determine
        the row to be updated.

    :returns:
    str - The new status for the row with the specified id.
    """
    select_existing_row_stmt = """
        SELECT status
        FROM orders
        WHERE id = %s;
    """
    logger.debug(f"Getting status for id: {id} with query: {select_existing_row_stmt}")

    with conn.cursor() as curr:
        try:
            curr.execute(select_existing_row_stmt, (id,))
            results = curr.fetchone()
        except UndefinedTable as err:
            logger.error(f"Orders table not found.\nPostgres error: {err}")
            raise

    if not results:
        raise RuntimeError(
            f"Could not fetch existing status. No data for id: {id} found."
        )

    current_status = results[0]

    new_statuses = [status for status in STATUS_VALUES if status != current_status]

    return random.choice(new_statuses)


def _get_row_count(conn: PGConnection) -> int:
    """
    Returns the number of rows in the orders table.

    :params:
    conn (PGConnection) - Postgres connection object.

    :returns:
    int - Number of rows in the orders table.
    """
    select_stmt = """
        SELECT count(*)
        FROM orders;
    """
    logger.debug(f"Getting status for id: {id} with query: {select_stmt}")

    with conn.cursor() as cur:
        try:
            cur.execute(select_stmt)
            results = cur.fetchone()
        except UndefinedTable as err:
            logger.error(f"Orders table not found.\nPostgres error: {err}")
            raise

    if not results:
        raise RuntimeError(
            "Error when querying the number of rows in the orders table"
        )

    return results[0]


@contextmanager
def get_db_connection(
    *,
    database: str,
    user: str,
    password: str,
    host: str = "localhost",
    port: int = 5432
) -> Generator[PGConnection, None, None]:
    """
    Creates connection to a local Postgres instance. We also automatically
    close the connection when this function is used as a context manager.

    :returns:
    Generator - Postgres connection object.
    """
    logger.info("Connecting to Postgres with user: {user}, database: {database}, host: {host}, port: {port}")
    conn = psycopg2.connect(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port
    )

    try:
        yield conn
    finally:
        conn.close()


def create_table(conn: PGConnection) -> None:
    """
    Creates the orders table if it does not exist.

    :params:
    conn (PGConnection) - Postgres connection object.
    """
    create_table_stmt = """
        CREATE TABLE IF NOT EXISTS orders (
            id                 TEXT    PRIMARY KEY,
            status             TEXT    NOT NULL,
            total_amount_cents INTEGER NOT NULL,
            created_at         BIGINT  NOT NULL,
            last_updated_at    BIGINT  NOT NULL
        );
    """
    logger.debug(f"Creating orders table if it does not exist with query: {create_table_stmt}")

    with conn.cursor() as cur:
        cur.execute(create_table_stmt)
        conn.commit()
        logger.info("Order table has been successfully created.")


def drop_table(conn: PGConnection) -> None:
    """
    Drops the orders table. If the table does not exist, this
    function does nothing.

    :params:
    conn (PGConnection) - Postgres connection object.
    """
    drop_table_stmt = """
        DROP TABLE IF EXISTS orders;
    """
    logger.debug(f"Dropping the orders table with query: {drop_table_stmt}")

    with conn.cursor() as cur:
        cur.execute(drop_table_stmt)
        conn.commit()
        logger.info("Order table has been successfully dropped.")


def insert_rows(conn: PGConnection, n: int) -> None:
    """
    Bulk insert a specified number of new rows into
    the orders table.

    :params:
    conn (PGConnection) - Postgres connection object.
    n (int) - Number of rows to insert into the orders table.
    """
    insert_stmt = """
        INSERT INTO orders
        VALUES (%s, %s, %s, %s, %s);
    """
    logger.debug(f"Inserting data into the orders table with query: {insert_stmt}")

    with conn.cursor() as cur:
        logger.debug(f"Inserting {n} records...")
        for _ in range(n):
            id = _generate_uuid()
            status = _generate_status()
            amount = _generate_amount()
            row_generated_at = _generate_timestamp()

            try:
                cur.execute(
                    insert_stmt,
                    (
                        id,
                        status,
                        amount,
                        row_generated_at,
                        row_generated_at
                    )
                )
                conn.commit()
                logger.info(f"Inserted record, id: {id}, status: {status}, amount: {amount}, created_at: {row_generated_at}, last_updated_at: {row_generated_at}")
            except UndefinedTable as err:
                logger.error(f"Orders table not found.\nPostgres error: {err}")
                raise


def update_rows(conn: PGConnection, n: int) -> None:
    """
    Update an existing set of rows in the orders table. The same row can be
    updated multiple times.

    :params:
    conn (PGConnection) - Postgres connection object.
    n (int) - Number of updates (at the row level) we make.
    """
    if not _check_table_data_exists(conn):
        raise RuntimeError("Orders table does not have any rows to update.")

    update_stmt = """
        UPDATE orders
        SET status = %s, last_updated_at = %s
        WHERE id = %s;
    """
    logger.debug(f"Updating {n} rows in the orders table with query: {update_stmt}")

    with conn.cursor() as cur:
        logger.debug(f"Updating {n} records...")
        for _ in range(n):
            id_to_update = _get_id(conn)
            new_status = _get_new_status(conn, id_to_update)
            last_updated_at = _generate_timestamp()

            try:
                cur.execute(
                    update_stmt,
                    (
                        new_status,
                        last_updated_at,
                        id_to_update
                    )
                )
                conn.commit()
                logger.info(f"Updated record, id: {id_to_update}, status: {new_status}")
            except UndefinedTable as err:
                logger.error(f"Orders table not found.\nPostgres error: {err}")
                raise


def delete_rows(conn: PGConnection, n: int) -> None:
    """
    Deletes a specified number of rows in the orders table. If the
    number of rows to be deleted is greater than the number of rows
    in the orders table, we override the value of n and set it to
    the number of rows in the table. If there are no rows
    in the table, then this function does nothing.

    :params:
    conn (PGConnection) - Postgres connection object.
    n (int) - Number of deletes (at the row level) we make.
    """
    if not _check_table_data_exists(conn):
        raise RuntimeError("Orders table does not have any rows to update.")
    
    row_count = _get_row_count(conn)
    
    delete_stmt = """
        DELETE FROM orders
        WHERE id = %s;
    """

    num_records_to_delete = min(n, row_count)
    logger.debug(f"Deleting {num_records_to_delete} rows in the orders table with query: {delete_stmt}")

    with conn.cursor() as cur:
        for _ in range(num_records_to_delete):
            try:
                id_to_delete = _get_id(conn)
                cur.execute(delete_stmt, (id_to_delete,))
                conn.commit()
                logger.info(f"Deleted record, id: {id_to_delete}")
            except UndefinedTable as err:
                logger.error(f"Orders table not found.\nPostgres error: {err}")
                raise
