SHELL := /bin/bash
PWD := $(shell pwd)

build:
	cd .. && docker build -f worker/Dockerfile -t "worker:latest" .

test-year-filter: build
	./generar-compose-test.sh 3 "YEAR_FILTER"
	docker compose -f test-compose.yaml up -d --build
	cd suite && python3 year_filter_test.py
	docker compose -f test-compose.yaml down -v
