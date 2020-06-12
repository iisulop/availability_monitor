## Kafka consumer

Consume Kafka topic continuously using `runner.py`. The consumer will pass the messages to a handler for further processing. Once running the consumer will wait for and pass new messages forward indefinitely.

The consumer's parameters will be passed via command line and the handler's parameters via environment variables. To run the Postgresql handler:
```
export POSTGRESQL_HOST=localhost
export POSTGRESQL_PORT=5432
export POSTGRESQL_USER=postgres
export POSTGRESQL_PASSWORD=1234
export POSTGRESQL_DBNAME=test-db-1
python runner.py --host localhost:9092 --topic test-topic-1 --client-id test-client-2 --producer postgresql --security-protocol SSL --ssl-cafile ca.pem --ssl-certfile service.cert --ssl-keyfile service.key
```

Alternatively `runner.py` does support `.env` files [python-dotenv](https://pypi.org/project/python-dotenv/) ie. reading the environment values from a file called `.env` in the same folder.

The Postgresql handler requires a database with the given `POSTGRESQL_DBNAME` to exist prior to running the command.
