import argparse
import os
import sys
from typing import List, Any, Dict

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import find_dotenv, load_dotenv

from listener.kafka_consumer import Kafka
import listener.handlers as handlers


def _params_from_env(prefix: str, param_names: List[str]) -> Dict[str, Any]:
    params = dict()
    missing_keys = list()
    for name in param_names:
        key = f"{prefix.upper()}_{name.upper()}"
        try:
            params[name] = os.environ[key]
        except KeyError:
            missing_keys.append(key)
    if missing_keys:
        raise KeyError(missing_keys)
    return params


def _main():
    parser = argparse.ArgumentParser(description="Consume given kafka topic passing the messages to given handler")
    parser.add_argument("--topic", required=True, help="Topic which to consume")
    parser.add_argument("--host", required=True, help="Kafka server host")
    parser.add_argument("--client-id", help="Kafka client id")
    parser.add_argument("--handler", required=True, choices=[p.__name__.lower() for p in handlers.HANDLERS])
    parser.add_argument("--security-protocol", help="Security protocol to use", choices=['SSL'])
    parser.add_argument("--ssl-cafile", help="Path to SSL CA file")
    parser.add_argument("--ssl-certfile", help="Path to SSL certificate file")
    parser.add_argument("--ssl-keyfile", help="Path to SSL key file")
    args = parser.parse_args(sys.argv[1:])

    load_dotenv(find_dotenv(), override=True)

    handler_class_name = args.handler.capitalize()
    try:
        handler_class = getattr(handlers, handler_class_name)
    except AttributeError:
        sys.stderr.write(f"No handler named {args.handler}")
        sys.exit(1)
    try:
        params = _params_from_env(args.handler, handler_class.init_params())
    except KeyError as e:
        sys.stderr.write(f"Missing environment variables for handler `{args.handler}``: {e}")
        sys.exit(1)

    handler = handler_class(**params)
    topic = args.topic
    bootstrap_servers = args.host
    try:
        client_id = args.client_id
    except TypeError:
        client_id = None
    security_protocol = args.security_protocol
    ssl_cafile = args.ssl_cafile
    ssl_certfile = args.ssl_certfile
    ssl_keyfile = args.ssl_keyfile

    scheduler = BackgroundScheduler(daemon=True)
    kafka_client = Kafka(
        handler,
        topic,
        bootstrap_servers,
        scheduler,
        client_id,
        security_protocol,
        ssl_cafile,
        ssl_certfile,
        ssl_keyfile,
    )
    scheduler.start()
    kafka_client.listen()


if __name__ == "__main__":
    _main()
