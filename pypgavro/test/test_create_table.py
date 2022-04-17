from decimal import Decimal

import pytest
from psycopg2.pool import ThreadedConnectionPool


@pytest.fixture
def get_connection_pool():
    conn_pool = ThreadedConnectionPool(
        minconn=1,
        maxconn=20,
        user="pg_user",
        password="pg_password",
        database="pypgavro"
    )
    yield conn_pool


@pytest.fixture
def create_table(get_connection_pool):
    pool = get_connection_pool
    conn = pool.getconn()
    cur = conn.cursor()
    cur.execute("drop table if exists public.test_table")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.test_table (
            col_smallint smallint,
            col_integer integer,
            col_bigint bigint,
            col_decimal decimal,
            col_numeric numeric,
            col_real real,
            col_double double precision,
            col_smallserial smallserial,
            col_serial serial,
            col_bigserial bigserial);""")
    conn.commit()


@pytest.fixture
def init_data(get_connection_pool, create_table):
    create_table
    pool = get_connection_pool
    conn = pool.getconn()
    cur = conn.cursor()
    sql = """INSERT INTO public.test_table (
        col_smallint, col_integer, col_bigint,
        col_decimal, col_numeric, col_real, col_double)
        VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    cur.execute(sql, (
        10, 10, 10, Decimal("10.10"), Decimal("10.10"), 10.10, 10.10))
    conn.commit()


def test_database_setup(get_connection_pool, init_data):
    init_data
    pool = get_connection_pool
    conn = pool.getconn()
    cur = conn.cursor()
    cur.execute("select count(*) from public.test_table")
    assert 1 == cur.fetchone()[0]
