import datetime
import json
from typing import Any, List

import psycopg2
from kafka.consumer.fetcher import ConsumerRecord

from listener.handlers.handler import Handler


class Postgresql(Handler):
    """
    Postgresql website availability producer.
    """
    class SqlCommands:
        # pylint: disable=missing-class-docstring,too-few-public-methods
        create_table_events = "CREATE TABLE IF NOT EXISTS events " \
                              "(id BIGSERIAL PRIMARY KEY, url VARCHAR(256), response_time TIMESTAMPTZ NOT NULL, " \
                              "result varchar(32) NOT NULL, status_code INT, elapsed FLOAT)"
        create_table_regex_results = "CREATE TABLE IF NOT EXISTS regex_results " \
                                     "(id SERIAL PRIMARY KEY, event_id BIGINT NOT NULL REFERENCES events(id), " \
                                     "regex VARCHAR(256) NOT NULL, found BOOLEAN NOT NULL)"
        insert_events = "INSERT INTO events (response_time, url, result, status_code, elapsed) " \
                        "VALUES (%(response_time)s, %(url)s, %(result)s, %(status_code)s, %(elapsed)s) RETURNING id"
        insert_regex_results = "INSERT INTO regex_results (event_id, regex, found) " \
                               "VALUES (%(event_id)s, %(regex)s, %(found)s)"

    def __init__(self, host: str, port: int, user: str, password: str, dbname: str):
        """
        Initialize Postgresql instance.

        :param host: Host address of the Postgresql instance
        :param port: Port number of the Postgresql instance
        :param user: Username to use when connecting to Postgres
        :param password: Password to use when connecting to Postgres
        :param dbname: Name of the database to use. Must exist before initializing any `Postgresql` objects
        """
        self._connection = self._connect(host, port, user, password, dbname)
        self._create_tables()

    @staticmethod
    def init_params() -> List[str]:
        """
        Get the parameters required by Postgresql `__init__`.
        :return: List of required parameters
        """
        return [
            "host",
            "port",
            "user",
            "password",
            "dbname"
        ]

    @staticmethod
    def _connect(host: str, port: int, user: str, password: str, dbname: str) -> Any:
        return psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)

    def _cur(self):
        return self._connection.cursor()

    def _create_tables(self):
        with self._cur() as cursor:
            cursor.execute(self.SqlCommands.create_table_events)
            cursor.execute(self.SqlCommands.create_table_regex_results)
            self._connection.commit()

    @staticmethod
    def _deserialize(msg: str) -> dict:
        deserialized = json.loads(msg)
        deserialized['response_time'] = datetime.datetime.fromisoformat(deserialized['response_time'])
        return deserialized

    def handle(self, msg: ConsumerRecord):
        """
        Forward information in the given message to Postgresql

        :param msg: The message from Kafka
        """
        msg = self._deserialize(msg.value.decode("utf-8"))
        with self._cur() as cursor:
            print(f"Adding event {msg}")
            cursor.execute(
                self.SqlCommands.insert_events,
                dict(
                    url=msg["url"],
                    response_time=msg["response_time"],
                    result=msg["result"],
                    elapsed=msg.get("elapsed"),
                    status_code=msg.get("status_code"),
                )
            )
            event_id = cursor.fetchone()[0]
            print("Adding regexes")
            for regex, matched in msg.get('regex_matches', dict()).items():
                cursor.execute(
                    self.SqlCommands.insert_regex_results,
                    dict(event_id=event_id, regex=regex, found=matched)
                )
            self._connection.commit()
            # Data types should be shared between the handlers and handlers
