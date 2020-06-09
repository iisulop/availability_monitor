import json
from enum import Enum

import psycopg2

from consumer.listener.consumer.consumer import Consumer


class Postgresql(Consumer):
    class SqlCommands(Enum):
        create_table_events="CREATE TABLE IF NOT EXISTS events " \
                            "(id BIGSERIAL PRIMARY KEY, url VARCHAR(256), response_time TIMESTAMPTZ, status_code INT)"
        create_table_regex_results="CREATE TABLE IF NOT EXISTS regex_results " \
                                   "(id SERIAL PRIMARY KEY, event_id BIGINT NOT NULL REFERENCES events(id), " \
                                   "regex VARCHAR(256), found BOOLEAN)"
        insert_events="INSERT INTO events (response_time, url, status_code) " \
                      "VALUES (%(response_time)s, %(url)s %(status_code)s) RETURNING id"
        insert_regex_results="INSERT INTO regex_results (event_id, regex, found) " \
                             "VALUES (%(event_id)s, %(regex)s, %(found)s)"

    def __init__(self, host: str, port: int, user: str, password: str, dbname: str, table_name: str):
        self._connection = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
        self._table_name = table_name
        self._create_table_if_needed()

    def _cur(self):
        return self._connection().cursor()

    def _create_table_if_needed(self):
        with self._cur() as cursor:
            cursor.execute(self.SqlCommands.create_table_events)
            cursor.execute(self.SqlCommands.create_table_regex_results)

    def _deserialize(self, msg: str) -> dict:
        return json.loads(msg)

    def handle(self, msg: str):
        msg = self._deserialize(msg)
        # insert into events
        with self._cur() as cursor:
            cursor.execute(
                self.SqlCommands.insert_events,
                dict(url=msg["url"], response_time=msg["response_time"], status_code=msg["status_code"])
            )
            event_id = cursor.fetchone()[0]
            # insert into regex_results if applicable
            for regex, matched in msg.get('found_match', dict()).items():
                cursor.execute(
                    self.SqlCommands.insert_regex_results,
                    dict(event_id=event_id, regex=regex, found=matched)
                )
            # Data types should be shared between the producer and consumer
