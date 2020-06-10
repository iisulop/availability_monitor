import argparse
import os
import sys
from typing import List, Any, Dict

import availability_monitor.monitor.producers as producers

from availability_monitor.monitor.monitor import Monitor


def params_from_env(param_names: List[str]) -> Dict[str, Any]:
    params = dict()
    missing_keys = list()
    for n in param_names:
        try:
            params[n] = os.environ[n.upper()]
        except KeyError:
            missing_keys.append(n.upper())
    if missing_keys:
        raise KeyError(missing_keys)
    return params


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check the status of a url sending the result to Kafka")
    parser.add_argument("--url", nargs=1, required=True, help="Url whose status to check")
    parser.add_argument("--regex", nargs=1, help="Regular expression to check for in the body of the url")
    parser.add_argument("--producer", nargs=1, required=True, choices=["kafka"])
    args = parser.parse_args(sys.argv[1:])

    producer_class_name = args.producer[0].capitalize()
    try:
        producer_class = getattr(producers, producer_class_name)
    except AttributeError:
        sys.stderr.write(f"No producer named {args.producer[0]}")
        sys.exit(1)
    try:
        params = params_from_env(producer_class.init_params())
    except KeyError as e:
        sys.stderr.write(f"Missing environment variables for producer `{args.producer[0]}``: {e}")
        sys.exit(1)

    try:
        regex = args.regex[0]
    except TypeError:
        # No regexes
        regex = None

    url = args.url[0]
    producer = producer_class(**params)
    monitor = Monitor(url, regex, producer)
    monitor.check()
    monitor.flush_producer()
