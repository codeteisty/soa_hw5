.PHONY: up down test logs
up:
	docker compose up --build -d

down:
	docker compose down

test:
	./scripts/test.sh

logs:
	docker compose logs -f producer aggregation generator
