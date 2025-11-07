.PHONY: help up down restart logs logs-kafka logs-init clean ps topics produce run build test

# Default target
help:
	@echo "📦 Ces.Kafka.Consumer.Resilient - Makefile Commands"
	@echo ""
	@echo "Docker Compose:"
	@echo "  make up          - Start all services (Kafka, Schema Registry, Kafka UI)"
	@echo "  make down        - Stop all services"
	@echo "  make restart     - Restart all services"
	@echo "  make clean       - Stop services and remove volumes (fresh start)"
	@echo "  make ps          - Show running containers"
	@echo ""
	@echo "Logs:"
	@echo "  make logs        - Show logs from all services"
	@echo "  make logs-kafka  - Show Kafka logs"
	@echo "  make logs-init   - Show init-kafka logs (topic creation)"
	@echo ""
	@echo "Kafka Operations:"
	@echo "  make topics      - List all Kafka topics"
	@echo "  make produce     - Produce 10 test messages"
	@echo "  make produce N=20 - Produce N test messages"
	@echo ""
	@echo "Application:"
	@echo "  make run         - Run the example consumer application"
	@echo "  make build       - Build the solution"
	@echo "  make test        - Run tests (if available)"
	@echo ""
	@echo "Quick Start:"
	@echo "  make up && make run"

# Docker Compose commands
up:
	@echo "🚀 Starting Kafka infrastructure..."
	docker-compose up -d
	@echo "✓ Services started!"
	@echo "  Kafka:          localhost:9092"
	@echo "  Schema Registry: localhost:8081"
	@echo "  Kafka UI:       http://localhost:8080"

down:
	@echo "🛑 Stopping services..."
	docker-compose down
	@echo "✓ Services stopped!"

restart:
	@echo "🔄 Restarting services..."
	docker-compose restart
	@echo "✓ Services restarted!"

clean:
	@echo "🧹 Cleaning up (removing volumes)..."
	docker-compose down -v
	@echo "✓ Clean complete!"

ps:
	@docker-compose ps

# Logs
logs:
	docker-compose logs -f

logs-kafka:
	docker logs -f kafka

logs-init:
	docker logs init-kafka

# Kafka operations
topics:
	@echo "📋 Kafka Topics:"
	@docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

produce:
	@echo "📤 Producing test JSON messages..."
	@./produce-test-messages.sh $(or $(N),10)

produce-avro:
	@echo "📤 Producing REAL Avro messages (binary)..."
	@./produce-avro-messages.sh $(or $(N),10)

# Application commands
run:
	@echo "🏃 Running example consumer..."
	dotnet run --project ./Kafka.Consumer.Resilient.Example/Kafka.Consumer.Resilient.Example.csproj

build:
	@echo "🔨 Building solution..."
	dotnet build ../Kafka.Consumer.Resilient.sln --configuration Release

test:
	@echo "🧪 Running tests..."
	dotnet test ../Kafka.Consumer.Resilient.sln

