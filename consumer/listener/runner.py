import argparse
import os
import sys
from typing import List, Any, Dict

from listener.kafka_consumer import Kafka
from listener.producers.postgresql import Postgresql
import listener.producers as producers


def params_from_env(prefix: str, param_names: List[str]) -> Dict[str, Any]:
    params = dict()
    missing_keys = list()
    for n in param_names:
        key = f"{prefix.upper()}_{n.upper()}"
        try:
            params[n] = os.environ[key]
        except KeyError:
            missing_keys.append(key)
    if missing_keys:
        raise KeyError(missing_keys)
    return params


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consume given kafka topic passing the messages to given producer")
    parser.add_argument("--topic", nargs=1, required=True, help="Topic which to consume")
    parser.add_argument("--host", nargs=1, required=True, help="Kafka server host")
    parser.add_argument("--client-id", nargs=1, help="Kafka client id")
    parser.add_argument("--producer", nargs=1, required=True, choices=["postgresql"])
    args = parser.parse_args(sys.argv[1:])

    producer_class_name = args.producer[0].capitalize()
    try:
        producer_class = getattr(producers, producer_class_name)
    except AttributeError:
        sys.stderr.write(f"No producer named {args.producer[0]}")
        sys.exit(1)
    try:
        params = params_from_env(args.producer[0], producer_class.init_params())
    except KeyError as e:
        sys.stderr.write(f"Missing environment variables for producer `{args.producer[0]}``: {e}")
        sys.exit(1)

    producer = Postgresql(**params)
    topic = args.topic[0]
    bootstrap_servers = args.host[0]
    try:
        client_id = args.client_id[0]
    except TypeError:
        client_id = None
    kafka_client = Kafka(
        producer,
        topic,
        bootstrap_servers,
        client_id
    )
    kafka_client.listen()
