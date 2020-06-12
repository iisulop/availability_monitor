## Website checker

Check web page status periodically using `runner.py`. The producer will be provided information about the url, result, elapsed time, status code and possible regex matches.

The web page monitor's parameters will be passed via command line and the producer's parameters via environment variables. To run the Kafka producer:
```
export KAFKA_HOST=localhost:9092
export KAFKA_TOPIC=test-topic-1
export KAFKA_CLIENT_ID=test-client-1
KAFKA_SECURITY_PROTOCOL=SSL
KAFKA_SSL_CAFILE=ca.pem
KAFKA_SSL_CERTFILE=service.cert
KAFKA_SSL_KEYFILE=service.key
python runner.py --url https://reddit.com --producer kafka
```

Alternatively `runner.py` does support `.env` files [python-dotenv](https://pypi.org/project/python-dotenv/) ie. reading the environment values from a file called `.env` in the same folder.

For Kafka the given topic will be created automatically if it does not exist prior to running the command.