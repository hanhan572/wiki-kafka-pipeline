"""Postgres helpers: connect with retry, create the schema."""
import logging
import pathlib
import time

import psycopg

from . import config

log = logging.getLogger(__name__)
SCHEMA_FILE = pathlib.Path(__file__).with_name("schema.sql")


def connect(retries: int = 30) -> psycopg.Connection:
    for _ in range(retries):
        try:
            return psycopg.connect(config.DATABASE_URL, autocommit=False, prepare_threshold=None)   # pooler-friendly
        except psycopg.OperationalError as exc:
            log.warning("waiting for Postgres (%s)", str(exc).strip()[:120])
            time.sleep(2)
    raise RuntimeError("Postgres not reachable")


def init_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(424242)")   # services start together: avoid DDL races
        cur.execute(SCHEMA_FILE.read_text())
    conn.commit()
