.PHONY: lint test install sdist

lint:
	flake8
	pylint monitor/

test:
	python -m pytest --cov=monitor/

install:
	pip install ".[test]"

sdist:
	python setup.py sdist

