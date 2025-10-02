SHELL := /bin/bash
PWD := $(shell pwd)

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
.PHONY: format

build: deps
	GOOS=linux go build -o bin/client ./client/main.go
	GOOS=linux go build -o bin/request_handler ./request_handler/main.go
	GOOS=linux go build -o bin/response_builder ./response_builder/main.go
	GOOS=linux go build -o bin/worker ./worker/main.go
.PHONY: build

docker-image:
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./request_handler/Dockerfile -t "request_handler:latest" .
	docker build -f ./response_builder/Dockerfile -t "response_builder:latest" .
	docker build -f ./worker/Dockerfile -t "worker:latest" .
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

middleware_tests:
	docker compose up -d middleware
	go test ./middleware

default-compose:
	./generar-compose.sh docker-compose-dev.yaml 2 2 2 2 2 2 2
