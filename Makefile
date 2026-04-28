.PHONY: up down logs ps topic-list produce-test

up:
	docker compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 20
	@echo "Kafka UI: http://localhost:8080"
	@echo "Schema Registry: http://localhost:8082"

down:
	docker compose down -v

logs:
	docker compose logs -f kafka

ps:
	docker compose ps

topic-list:
	docker exec adstream-kafka kafka-topics --bootstrap-server localhost:9092 --list

produce-test:
	docker exec -it adstream-kafka kafka-console-producer \
		--bootstrap-server localhost:9092 \
		--topic ad.impressions.raw

consume-test:
	docker exec -it adstream-kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic ad.impressions.raw \
		--from-beginning

test:
    python -m pytest tests/test_models.py tests/test_producers.py tests/test_consumers.py -v --tb=short	