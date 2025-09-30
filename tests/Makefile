SHELL := /bin/bash
PWD := $(shell pwd)

build:
	cd .. && docker build -f worker/Dockerfile -t "worker:latest" .

test-year-filter: build
	# We generate the compose in the parent directory due
	# to relative paths
	cd .. && ./generar-compose.sh test-compose.yaml 3 1 0
	cd .. && docker compose -f test-compose.yaml up -d --build
	cd suite && python3 test_year_filter.py
	MAKE cleanup

test-hour-filter: build
	# We generate the compose in the parent directory due
	# to relative paths
	cd .. && ./generar-compose.sh test-compose.yaml 0 3 1
	cd .. && docker compose -f test-compose.yaml up -d --build
	cd suite && python3 test_hour_filter.py
	MAKE cleanup

test-amount-filter: build
	# We generate the compose in the parent directory due
	# to relative paths
	cd .. && ./generar-compose.sh test-compose.yaml 0 0 3
	cd .. && docker compose -f test-compose.yaml up -d --build
	cd suite && python3 test_amount_filter.py
	MAKE cleanup

test-first-query: build
	# We generate the compose in the parent directory due
	# to relative paths
	cd .. && ./generar-compose.sh test-compose.yaml 1 2 3
	cd .. && docker compose -f test-compose.yaml up -d --build
	cd suite && python3 test_first_query.py
	MAKE cleanup

cleanup:
	docker compose -f ../test-compose.yaml down -v
	rm ../test-compose.yaml
