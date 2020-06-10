import datetime
import json

import psycopg2
import pytest

from consumer.listener.consumer import Consumer
from consumer.listener.producers.postgresql import Postgresql

@pytest.fixture(scope="session")
def postgresql_db_name():
    return "test_postgresql_db"

@pytest.fixture(scope="session")
def postgresql_connect_data(postgresql_db_name):
    return dict(
        host="localhost",
        port=5432,
        user="postgres",
        password="1234",
        dbname=postgresql_db_name,
    )

@pytest.fixture(scope="session")
def postgresql_db(postgresql_db_name, postgresql_connect_data):
    params = postgresql_connect_data.copy()
    del params["dbname"]
    connection = psycopg2.connect(**params)
    connection.autocommit = True
    try:
        cursor = connection.cursor()
        cursor.execute(f"DROP DATABASE {postgresql_db_name}")
        cursor.execute(f"CREATE DATABASE {postgresql_db_name}")
        cursor.close()
    except Exception as e:
        if "DuplicateDatabase" not in str(type(e)):
            raise


@pytest.fixture(scope="function")
def postgresql(postgresql_db, postgresql_connect_data):
    sql = Postgresql(**postgresql_connect_data)
    yield sql

class TestSqlProducer:
    @staticmethod
    def _serialize(msg):
        return json.dumps(msg)

    def test_sql_init(self, postgresql_db, postgresql_db_name, postgresql_connect_data):
        sql = Postgresql(**postgresql_connect_data)
        assert isinstance(sql, Postgresql)
        assert isinstance(sql, Consumer)
        # Check for multiple instance creation and instantiating when tables exist
        sql = Postgresql(**postgresql_connect_data)
        assert isinstance(sql, Postgresql)
        assert isinstance(sql, Consumer)

    def test_sql_handle_ok(self, postgresql):
        msg = dict(
            result="ok",
            url="https://urlgoeshere.com/page",
            elapsed=0.1,
            response_time=datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
            status_code=200,
        )
        postgresql.handle(self._serialize(msg))

        msg = dict(
            result="ok",
            url="https://urlgoeshere.com/page",
            response_time=datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
            status_code=200,
            regex_matches={
                "regex here": True,
                "another regex": False,
            }
        )
        postgresql.handle(self._serialize(msg))

    def test_sql_handle_error(self, postgresql):
        msg = dict(
            result="error",
            url="https://urlgoeshere.com/page",
            response_time=datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
            status_code=400,
        )
        postgresql.handle(self._serialize(msg))

    def test_sql_handle_failure(self, postgresql):
        msg = dict(
            result="failure",
            url="https://urlgoeshere.com/page",
            response_time=datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
        )
        postgresql.handle(self._serialize(msg))
