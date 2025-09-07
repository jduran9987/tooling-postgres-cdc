import os
from pathlib import Path

from dotenv import load_dotenv

from database import _check_table_data_exists, get_db_connection


CONFIG_PATH = Path(__file__).parents[1] / ".env"
load_dotenv(CONFIG_PATH)
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_PORT = int(os.environ["POSTGRES_PORT"])

with get_db_connection(
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
) as conn:
    _check_table_data_exists(conn)
