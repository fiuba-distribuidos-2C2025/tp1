.PHONY: build

deps:
	go mod tidy
	go mod vendor

build:
	docker build -f ./Dockerfile -t "worker:latest" ..

up:
	docker compose -f ../docker-compose-worker.yaml up -d --build
.PHONY: docker-compose-up

down:
	docker compose -f../docker-compose-worker.yaml stop -t 10
	docker compose -f ../docker-compose-worker.yaml down
.PHONY: docker-compose-down