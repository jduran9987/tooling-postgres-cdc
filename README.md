# Tooling Postgres CDC

This application helps automate data operations against a Postgres database.  
The goal is to support local data engineering projects by providing a data source that can be easily populated and manipulated.

It is especially useful for experimenting with **Change Data Capture (CDC)** concepts and practicing database operations in a reproducible environment.

---

## Features

- Create and drop an `orders` table.
- Insert synthetic rows with randomized IDs, statuses, amounts, and timestamps.
- Update existing rows with new statuses and timestamps.
- Delete rows safely, ensuring the table never underflows.
- Works against both a **local Postgres container** (via Docker Compose) or **any external Postgres instance** by editing the `.env` file.
- Exposes a **CLI interface** for simple workflows.

---

## Requirements

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/) (for local DB).
- Python 3.12+ with [uv](https://github.com/astral-sh/uv) or [pip](https://pip.pypa.io).

---

## Getting Started

### 1. Clone the repo

```bash
git clone https://github.com/your-org/tooling-postgres-cdc.git
cd tooling-postgres-cdc
```

### 2. Configure environment variables

Copy the `.env` file (or create it if missing):

```dotenv
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

- These values match the **local Docker Compose setup**.
- To connect to another Postgres instance (e.g., cloud provider), change `POSTGRES_HOST`, `POSTGRES_PORT`, and credentials accordingly.

---

## Running Postgres Locally

A Postgres 15 container is included via Docker Compose.

Start it with:

```bash
docker compose up -d
```

This will:

- Run Postgres on port `5432`.
- Persist data in a Docker volume (`postgres_data`).

Stop it with:

```bash
docker compose down
```

---

## Installation

Use [uv](https://github.com/astral-sh/uv) to install dependencies:

```bash
uv sync --locked --all-extras --dev
```

Or with pip:

```bash
pip install -r requirements.txt
```

---

## Usage

The app is driven by a CLI entrypoint (`main.py`). It supports the following flags:

```bash
uv run python src/main.py --help
```

### CLI Options

- `--action {insert, update, delete}`  
  The database action to execute.
- `--num-rows N`  
  Number of rows to affect. Required if `--action` is provided.
- `--clean`  
  Drops the `orders` table (ignores `--action` and `--num-rows`).

---

### Examples

#### Insert rows

```bash
uv run python src/main.py --action insert --num-rows 10
```

Creates the `orders` table (if not exists) and inserts 10 random rows.

#### Update rows

```bash
uv run python src/main.py --action update --num-rows 5
```

Randomly updates the status of 5 existing rows.

#### Delete rows

```bash
uv run python src/main.py --action delete --num-rows 3
```

Deletes up to 3 rows (limited by row count in the table).

#### Drop the table

```bash
uv run python src/main.py --clean
```

Drops the `orders` table completely.

---

## Project Layout

```
src/
 ├── database.py   # Database helpers (CRUD, row generators)
 ├── loggers.py    # JSON logger configuration
 └── main.py       # CLI entrypoint
tests/
 └── test_database.py  # Unit tests with mocks
docker-compose.yaml   # Local Postgres service
.env                  # Environment config for DB connection
pyproject.toml        # Project metadata and dependencies
```

---

## Extending

- Modify `src/database.py` to add new operations or schemas.
- Swap out the `.env` values to point to remote Postgres instances.
- Logs are emitted in structured JSON (via `loggers.py`), making it easy to feed into observability tools.

---
