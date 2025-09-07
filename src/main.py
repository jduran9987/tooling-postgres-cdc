"""
Command-line entrypoint for managing the Postgres orders table.

Parses CLI arguments, loads configuration from .env, and delegates to
database functions to create, insert, update, delete, or drop rows.
"""
import argparse
import os
from pathlib import Path

from dotenv import load_dotenv

from database import (
    create_table, delete_rows, drop_table,
    insert_rows, get_db_connection, update_rows
)
from loggers import logger


CONFIG_PATH = Path(__file__).parents[1] / ".env"
print(CONFIG_PATH)
load_dotenv(CONFIG_PATH)
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_PORT = int(os.environ["POSTGRES_PORT"])


def resolve_args() -> argparse.Namespace:
    """
    Parses CLI arguments and performs validation.

    :returns:
    Namespace - Object containing all passed in CLI arguments.
    """
    parser = argparse.ArgumentParser(
        description="Data generator for Postgres-CDC project."
    )

    logger.debug("Parsing CLI arguments...")

    parser.add_argument(
        "--action",
        choices=["insert", "update", "delete"],
        help="The database action this script will execute. Valid values are `insert`, `update`, and `delete`."
    )
    parser.add_argument(
        "--num-rows",
        type=int,
        help="The number of rows to affected by the intended `action`."
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Drops the orders table (no value needed)."
    )

    args = parser.parse_args()

    if args.action and args.num_rows is None:
        parser.error("--num-rows is required when --action flag is provided.")
    
    if args.clean and args.action:
        parser.error("--clean should be the only flag present.")
    
    logger.info(f"CLI arguments: {args}")

    return args


def main() -> None:
    """
    Application entrypoint.
    """
    args = resolve_args()
    action = args.action
    num_rows = args.num_rows
    clean = args.clean

    with get_db_connection(
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    ) as conn:
        create_table(conn)

        if clean:
            drop_table(conn)

        if action == "insert":
            insert_rows(conn, num_rows)
        
        if action == "update":
            update_rows(conn, num_rows)
        
        if action == "delete":
            delete_rows(conn, num_rows)


if __name__ == "__main__":
    main()
