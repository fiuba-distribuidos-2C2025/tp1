middleware_tests:
	docker compose up -d
	go test ./middleware
