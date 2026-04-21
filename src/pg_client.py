"""Cliente PostgreSQL para o banco legado (psycopg 3, server-side cursor)."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg.rows import dict_row

from .config import PgConfig


@contextmanager
def conectar(pg: PgConfig) -> Iterator[psycopg.Connection]:
    if not pg.host:
        raise ValueError("PG host não configurado — rode `octa-migracao parear` antes.")
    conn = psycopg.connect(
        host=pg.host,
        port=pg.port,
        dbname=pg.database,
        user=pg.user,
        password=pg.password,
        sslmode="require" if pg.ssl else "prefer",
        connect_timeout=10,
        row_factory=dict_row,
    )
    try:
        yield conn
    finally:
        conn.close()


def testar(pg: PgConfig) -> dict:
    """Testa conexão e devolve metadados básicos."""
    with conectar(pg) as conn, conn.cursor() as cur:
        cur.execute("SELECT version() AS versao, current_database() AS db, current_user AS usr")
        meta = cur.fetchone() or {}
        cur.execute(
            "SELECT count(*)::bigint AS qtd "
            "FROM information_schema.tables "
            "WHERE table_schema NOT IN ('pg_catalog','information_schema')"
        )
        qtd_row = cur.fetchone() or {}
        meta["qtd_tabelas"] = qtd_row.get("qtd")
    return meta


def stream_rows(
    pg: PgConfig, sql: str, params: tuple = (), batch_size: int = 1000
) -> Iterator[list[dict]]:
    """Yield batches de linhas usando server-side cursor (não carrega tudo em memória)."""
    with conectar(pg) as conn:
        with conn.cursor(name="octa_migracao_cur") as cur:
            cur.itersize = batch_size
            cur.execute(sql, params)
            batch: list[dict] = []
            for row in cur:
                batch.append(dict(row))
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch