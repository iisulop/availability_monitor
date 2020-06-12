.PHONY: bdist lint test

lint:
	flake8
	pylint listener/

test:
	python -m pytest --cov=listener/

install:
	pip install ".[test]"

sdist:
	python setup.py sdist

