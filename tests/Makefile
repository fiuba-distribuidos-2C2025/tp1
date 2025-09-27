SHELL := /bin/bash
PWD := $(shell pwd)

build:
	cd .. && docker build -f worker/Dockerfile -t "worker:latest" .

test-year-filter: build
	# We generate the compose in the parent directory due
	# to relative paths
	cd .. && ./generar-compose.sh test-compose.yaml 3
	cd .. && docker compose -f test-compose.yaml up -d --build
	cd suite && python3 year_filter_test.py
	MAKE cleanup

cleanup:
	docker compose -f ../test-compose.yaml down -v
	rm ../test-compose.yaml
