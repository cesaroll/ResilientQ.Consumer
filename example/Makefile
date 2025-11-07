.PHONY: help up up-with-consumer down restart logs logs-kafka logs-init logs-consumer clean ps topics produce produce-avro run build test build-docker

# Default target
help:
	@echo "📦 Ces.Kafka.Consumer.Resilient - Makefile Commands"
	@echo ""
	@echo "Docker Compose:"
	@echo "  make up                - Start infrastructure (Kafka, Schema Registry, Kafka UI)"
	@echo "  make up-with-consumer  - Start all services including Example consumer"
	@echo "  make down              - Stop all services"
	@echo "  make restart           - Restart all services"
	@echo "  make clean             - Stop services and remove volumes (fresh start)"
	@echo "  make ps                - Show running containers"
	@echo ""
	@echo "Logs:"
	@echo "  make logs              - Show logs from all services"
	@echo "  make logs-kafka        - Show Kafka logs"
	@echo "  make logs-init         - Show init-kafka logs (topic creation)"
	@echo "  make logs-consumer     - Show Example consumer logs"
	@echo ""
	@echo "Kafka Operations:"
	@echo "  make topics            - List all Kafka topics"
	@echo "  make produce           - Produce 10 test JSON messages"
	@echo "  make produce N=20      - Produce N test JSON messages"
	@echo "  make produce-avro      - Produce 10 test Avro messages"
	@echo "  make produce-avro N=20 - Produce N test Avro messages"
	@echo ""
	@echo "Application:"
	@echo "  make run               - Run the example consumer locally (not in Docker)"
	@echo "  make build             - Build the solution"
	@echo "  make build-docker      - Build Docker image for Example consumer"
	@echo "  make test              - Run tests (if available)"
	@echo ""
	@echo "Quick Start (Local):"
	@echo "  make up && make run"
	@echo ""
	@echo "Quick Start (Docker):"
	@echo "  make up-with-consumer"

# Docker Compose commands
up:
	@echo "🚀 Starting Kafka infrastructure..."
	docker-compose up -d kafka schema-registry kafka-rest init-kafka kafka-ui
	@echo "✓ Services started!"
	@echo "  Kafka:           localhost:9092"
	@echo "  Schema Registry: localhost:8081"
	@echo "  Kafka REST:      localhost:8082"
	@echo "  Kafka UI:        http://localhost:8080"
	@echo ""
	@echo "💡 Tip: Run 'make up-with-consumer' to also start the Example consumer"

up-with-consumer:
	@echo "🚀 Starting all services including Example consumer..."
	make build-docker; docker-compose up -d
	@echo "✓ All services started!"
	@echo "  Kafka:            localhost:9092"
	@echo "  Schema Registry:  localhost:8081"
	@echo "  Kafka REST:       localhost:8082"
	@echo "  Kafka UI:         http://localhost:8080"
	@echo "  Example Consumer: Running in container"
	@echo ""
	@echo "💡 View consumer logs with: make logs-consumer"

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

logs-consumer:
	docker logs -f example-consumer

# Kafka operations
topics:
	@echo "📋 Kafka Topics:"
	@docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

produce:
	@echo "📤 Producing test JSON messages..."
	@./produce-test-messages.sh $(or $(N),10)

produce-avro:
	@echo "📤 Producing Avro messages via REST Proxy..."
	@./produce-avro-messages.sh $(or $(N),10)

# Application commands
run:
	@echo "🏃 Running example consumer..."
	dotnet run --project ./Example/Example.csproj

build:
	@echo "🔨 Building solution..."
	dotnet build ../Kafka.Consumer.Resilient.sln --configuration Release

test:
	@echo "🧪 Running tests..."
	dotnet test ../Kafka.Consumer.Resilient.sln

build-docker:
	@echo "🐳 Building Docker image for Example consumer..."
	docker-compose build example-consumer
	@echo "✓ Docker image built successfully!"

