import argparse
import os
import sys
from typing import List, Any, Dict

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv, find_dotenv

import monitor.handlers as handlers

from monitor.web_monitor import WebMonitor


def _params_from_env(prefix, param_names: List[str]) -> Dict[str, Any]:
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
    parser = argparse.ArgumentParser(description="Check the status of a url sending the result to Kafka")
    parser.add_argument("--url", required=True, help="Url whose status to check")
    parser.add_argument("--regex", help="Regular expression to check for in the body of the url", default=None)
    parser.add_argument(
        "--handler",
        required=True,
        choices=[p.__name__.lower() for p in handlers.HANDLERS],
        help="The handler to pass the results to"
    )
    parser.add_argument("--period", type=float, help="The time period between checks in seconds", default=None)
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

    regex = args.regex
    url = args.url
    handler = handler_class(**params)
    period = args.period
    scheduler = None
    if period is not None:
        scheduler = BackgroundScheduler(daemon=False)

    monitor = WebMonitor(url, handler, regex, period, scheduler)
    monitor.schedule()
    scheduler.start()
    monitor.flush()


if __name__ == "__main__":
    _main()
