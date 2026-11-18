PYTHON ?= .venv/bin/python
PIP ?= .venv/bin/pip
export JAVA_HOME ?= $(shell brew --prefix openjdk@17 2>/dev/null)/libexec/openjdk.jdk/Contents/Home
export PYTHONPATH := src:$(PYTHONPATH)

.PHONY: setup download run demo test clean

setup:
	python3 -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements-dev.txt
	$(PIP) install -e .

download:
	$(PYTHON) scripts/download_data.py

run:
	$(PYTHON) -m taxi_pipeline.pipeline

demo:
	$(PYTHON) scripts/actions_vs_transformations.py

test:
	$(PYTHON) -m pytest

clean:
	rm -rf artifacts/run spark-warehouse metastore_db derby.log
