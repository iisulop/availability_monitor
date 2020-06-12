import datetime
import json
import os
import random
from collections import namedtuple

import psycopg2
import pytest

from listener.handlers.handler import Handler
from listener.handlers.postgresql import Postgresql


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
        try:
            cursor.execute(f"DROP DATABASE {postgresql_db_name}")
        except Exception as e:
            if f'database "{postgresql_db_name}" does not exist' not in str(e):
                raise
        cursor.execute(f"CREATE DATABASE {postgresql_db_name}")
        cursor.close()
    except Exception as e:
        if "DuplicateDatabase" not in str(type(e)):
            raise


@pytest.fixture(scope="function")
def postgresql(postgresql_db, postgresql_connect_data):
    sql = Postgresql(**postgresql_connect_data)
    yield sql


MockConsumerRecord = namedtuple("MockConsumerRecord", ["value"])


@pytest.mark.skipif(os.environ.get("CI", None) is not None, reason="No Postgresql DB available in CI")
class TestSqlProducer:
    @staticmethod
    def _serialize_to_consumer_record(msg):
        return MockConsumerRecord(value=json.dumps(msg).encode("utf-8"))

    def test_sql_init(self, postgresql_db, postgresql_db_name, postgresql_connect_data):
        sql = Postgresql(**postgresql_connect_data)
        assert isinstance(sql, Postgresql)
        assert isinstance(sql, Handler)
        # Check for multiple instance creation and instantiating when tables exist
        sql = Postgresql(**postgresql_connect_data)
        assert isinstance(sql, Postgresql)
        assert isinstance(sql, Handler)

    def test_sql_handle_ok(self, postgresql):
        msg = dict(
            result="ok",
            url="https://urlgoeshere.com/page",
            elapsed=random.randint(1, 3000) / 1000.0,
            response_time=datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
            status_code=200,
        )
        postgresql.handle(self._serialize_to_consumer_record(msg))

        msg = dict(
            result="ok",
            url="https://urlgoeshere.com/page",
            response_time=datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
            status_code=200,
            elapsed=random.randint(1, 3000) / 1000.0,
            regex_matches={
                "regex here": True,
                "another regex": False,
            }
        )
        postgresql.handle(self._serialize_to_consumer_record(msg))

    def test_sql_handle_error(self, postgresql):
        msg = dict(
            result="error",
            url="https://urlgoeshere.com/page",
            elapsed=random.randint(1, 3000) / 1000.0,
            response_time=datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
            status_code=400,
        )
        postgresql.handle(self._serialize_to_consumer_record(msg))

    def test_sql_handle_failure(self, postgresql):
        msg = dict(
            result="failure",
            url="https://urlgoeshere.com/page",
            response_time=datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
        )
        postgresql.handle(self._serialize_to_consumer_record(msg))
