SHELL := /bin/bash
PWD := $(shell pwd)

default: build

all:

deps:
	go mod tidy
	go mod vendor

fmt:
	gofmt -s -w ./worker/
	gofmt -s -w ./middleware/
.PHONY: fmt

build: deps
	GOOS=linux go build -o bin/worker ./worker/main.go
	GOOS=linux go build -o bin/middleware ./middleware/main.go
.PHONY: build

docker-image:
	docker build -f ./worker/Dockerfile -t "worker:latest" .
	docker build -f ./middleware/Dockerfile -t "middleware:latest" .
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
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs
