SHELL := /bin/bash
PWD := $(shell pwd)

CLIENT?=alice
EXPECTED_CLIENT_RESULTS?=multiclient_1
CHAOS_INTERVAL?=30s
NUM_TO_KILL?=1

default: build

all:

deps:
	go mod tidy
	go mod vendor

format:
	gofmt -s -w ./client/
	gofmt -s -w ./request_handler/
	gofmt -s -w ./middleware/
	gofmt -s -w ./response_builder/
	gofmt -s -w ./worker/
	gofmt -s -w ./protocol/
	gofmt -s -w ./proxy/
.PHONY: format

build: deps
	GOOS=linux go build -o bin/client ./client/main.go
	GOOS=linux go build -o bin/request_handler ./request_handler/main.go
	GOOS=linux go build -o bin/response_builder ./response_builder/main.go
	GOOS=linux go build -o bin/worker ./worker/main.go
	GOOS=linux go build -o bin/proxy ./proxy/main.go
.PHONY: build

docker-image:
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./request_handler/Dockerfile -t "request_handler:latest" .
	docker build -f ./response_builder/Dockerfile -t "response_builder:latest" .
	docker build -f ./worker/Dockerfile -t "worker:latest" .
	docker build -f ./proxy/Dockerfile -t "proxy:latest" .
	# Execute this command from time to time to clean up intermediate stages generated
	# during client build (your hard drive will like this :) ). Don't left uncommented if you
	# want to avoid rebuilding client image every time the docker-compose-up command
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

docker-compose-up: docker-image
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose-dev.yaml stop -t 10
	docker compose -f docker-compose-dev.yaml down -v
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs

clean_results:
	rm -rf ./results/*

middleware_tests:
	docker compose -f docker-compose-dev.yaml up -d rabbit
	go test ./middleware

generar-compose:
	./generar-compose.sh docker-compose-dev.yaml setup.dev

compare_reduced_results:
	python3 scripts/compare_results.py ./results/client_$(CLIENT) ./expected_results/reduced

compare_full_results:
	python3 scripts/compare_results.py ./results/client_$(CLIENT) ./expected_results/full

compare_results_multiclient:
	python3 scripts/compare_results.py ./results/client_$(CLIENT) ./expected_results/$(EXPECTED_CLIENT_RESULTS)

download_reduced_dataset:
	./scripts/load_dataset.sh 1 0

download_full_dataset:
	./scripts/load_dataset.sh 0 0

download_multiclient_dataset:
	./scripts/load_dataset.sh 0 1

run_multiclient_test: docker-image clean_results
	docker compose -f docker-compose-multiclient-test.yaml up -d --build

stop_multiclient_test:
	docker compose -f  docker-compose-multiclient-test.yaml stop -t 10
	docker compose -f  docker-compose-multiclient-test.yaml down -v

run_client: clean_results
	docker run -d \
      --name client${CLIENT} \
      --network tp1_testing_net \
      --entrypoint /client \
      -v ./client:/config \
      -v ./data:/data \
      -v ./results:/results \
      -e CLIENT_ID=${CLIENT} \
      client:latest

.PHONY: run_multiclient_test

integration_test:
	cd tests && go test

.PHONY: integration_test

chaos:
	./scripts/chaos.sh ${CHAOS_INTERVAL} 'aggregator\|joiner\|grouper\|filter' ${NUM_TO_KILL}
.PHONY: chaos
