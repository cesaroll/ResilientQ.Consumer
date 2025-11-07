# Ces.Kafka.Consumer.Resilient - Example Application

This is an example console application demonstrating how to use the `Ces.Kafka.Consumer.Resilient` library.

## What This Example Does

This example:
- Creates a resilient Kafka consumer that processes order messages
- Configures 2 concurrent consumers
- Implements retry logic with 3 retry attempts
- Simulates various processing scenarios (success, retryable errors, fatal errors)
- Demonstrates JSON message deserialization

## Prerequisites

Before running this example, you need:

1. **Docker** and **Docker Compose**
2. **.NET 10.0 SDK**

## Quick Start

### Option 1: Run Everything in Docker (Recommended)

The easiest way to run this example is using Docker Compose:

```bash
# Start all services including the Example consumer
make up-with-consumer

# Produce test messages
make produce

# View consumer logs
make logs-consumer

# Monitor in Kafka UI
open http://localhost:8080

# Stop everything when done
make down
```

### Option 2: Run Consumer Locally

Run the consumer locally while Kafka runs in Docker:

```bash
# 1. Start Kafka infrastructure only
make up

# 2. Run the consumer locally (in one terminal)
make run

# 3. Produce test messages (in another terminal)
make produce

# 4. Monitor in Kafka UI
open http://localhost:8080

# 5. Stop everything when done
make down
```

## Available Makefile Commands

### Docker Compose
- `make up` - Start infrastructure (Kafka, Schema Registry, Kafka UI)
- `make up-with-consumer` - Start all services including Example consumer
- `make down` - Stop all services
- `make restart` - Restart all services
- `make clean` - Stop services and remove volumes (fresh start)
- `make ps` - Show running containers

### Logs
- `make logs` - Show logs from all services
- `make logs-kafka` - Show Kafka logs
- `make logs-init` - Show init-kafka logs (topic creation)
- `make logs-consumer` - Show Example consumer logs

### Kafka Operations
- `make topics` - List all Kafka topics
- `make produce` - Produce 10 test messages
- `make produce N=20` - Produce N test messages

### Application
- `make run` - Run the example consumer locally (not in Docker)
- `make build` - Build the solution
- `make build-docker` - Build Docker image for Example consumer
- `make test` - Run tests (if available)

## What to Observe

The example simulates three scenarios:

1. **Success (80%)**: Message is processed successfully
2. **Retryable Error (10%)**: Simulates a temporary database connection issue
   - Message will be moved to retry topics: `orders.retry.1`, `orders.retry.2`, `orders.retry.3`
   - Each retry has a 2-second delay
3. **Non-Retryable Error (10%)**: Simulates invalid data
   - Message is sent directly to error topic: `orders.error`

## Configuration

### Local Configuration
The consumer is configured via `Example/appsettings.json` for local development:

```json
{
  "KafkaConsumer": {
    "TopicName": "orders",
    "ConsumerNumber": 2,
    "GroupId": "order-processor-group",
    "SchemaRegistryUrl": "http://localhost:8081",
    "BootstrapServers": "localhost:9092",
    "Error": {
      "TopicName": "orders.error"
    },
    "RetryPolicy": {
      "Delay": 2000,
      "RetryAttempts": 3
    }
  }
}
```

### Docker Configuration
When running in Docker, the consumer uses `Example/appsettings.Docker.json`:

**Key differences from local configuration:**
- Uses service names instead of `localhost`: `kafka:29092` and `http://schema-registry:8081`
- Uses internal Kafka port `29092` instead of exposed port `9092`
- Different consumer group ID to avoid conflicts: `order-processor-group-docker`

```json
{
  "KafkaConsumer": {
    "TopicName": "orders",
    "ConsumerNumber": 2,
    "GroupId": "order-processor-group-docker",
    "SchemaRegistryUrl": "http://schema-registry:8081",
    "BootstrapServers": "kafka:29092",
    "Error": {
      "TopicName": "orders.error"
    },
    "RetryPolicy": {
      "Delay": 2000,
      "RetryAttempts": 3
    }
  }
}
```

### How Configuration Selection Works

The application automatically selects the correct configuration file based on the environment:

**Local development** (default):
- Uses `appsettings.json`
- Connects to `localhost:9092` and `localhost:8081`
- No environment variable needed

**Docker deployment**:
- The `docker-compose.yml` sets `ASPNETCORE_ENVIRONMENT=Docker` and `DOTNET_ENVIRONMENT=Docker`
- .NET automatically loads `appsettings.Docker.json` in addition to `appsettings.json`
- Values in `appsettings.Docker.json` override those in `appsettings.json`
- Connects to `kafka:29092` and `schema-registry:8081` (Docker service names)

**Why different endpoints?**
- **Local**: Application runs on your machine, Kafka runs in Docker → use `localhost`
- **Docker**: Application runs in Docker, Kafka runs in Docker → use Docker service names

## Infrastructure Services

When you run `make up`, the following services are started:

- **Kafka** on `localhost:9092` (KRaft mode - no Zookeeper needed)
- **Schema Registry** on `localhost:8081`
- **Kafka REST Proxy** on `localhost:8082` (for HTTP-based Avro message production)
- **Kafka UI** on `http://localhost:8080`

Topics are automatically created:
- `orders` (main topic)
- `orders.retry.1`, `orders.retry.2`, `orders.retry.3` (retry topics)
- `orders.error` (dead letter queue)

### Producing Avro Messages via REST Proxy

You can produce Avro messages using simple HTTP requests:

```bash
# Produce an Avro message using curl
curl -X POST http://localhost:8082/topics/orders \
  -H "Content-Type: application/vnd.kafka.avro.v2+json" \
  -H "Accept: application/vnd.kafka.v2+json" \
  -d '{
    "value_schema": "{\"type\":\"record\",\"name\":\"OrderMessage\",\"namespace\":\"Example\",\"fields\":[{\"name\":\"OrderId\",\"type\":\"string\"},{\"name\":\"CustomerId\",\"type\":\"string\"},{\"name\":\"Amount\",\"type\":\"double\"},{\"name\":\"OrderDate\",\"type\":\"string\"},{\"name\":\"Status\",\"type\":\"string\"}]}",
    "records": [
      {
        "value": {
          "OrderId": "ORD-001",
          "CustomerId": "CUST-100",
          "Amount": 99.99,
          "OrderDate": "2025-11-07T10:00:00Z",
          "Status": "Pending"
        }
      }
    ]
  }'
```

This makes it easy to produce Avro messages without needing a separate producer application.

## Monitoring

### Using Kafka UI (Recommended)
Open http://localhost:8080 to see:
- All topics and messages
- Consumer groups and lag
- Schema Registry
- Real-time message flow

### Using Command Line

```bash
# List all topics
make topics

# Monitor main topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning

# Monitor error topic with headers
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders.error \
  --from-beginning \
  --property print.headers=true
```

## Project Structure

```
example/
├── Example/
│   ├── OrderMessage.cs              # Message model
│   ├── OrderMessageHandler.cs       # Handler implementation
│   ├── Program.cs                   # Application entry point
│   └── appsettings.json            # Configuration
├── Makefile                         # Build and run commands
├── docker-compose.yml               # Kafka infrastructure
├── produce-test-messages.sh         # Helper script for producing messages
└── README.md                        # This file
```

## Troubleshooting

### Connection Refused

If you see connection errors:
- Ensure Kafka is running: `make ps`
- Check Kafka logs: `make logs-kafka`
- Verify no firewall is blocking connections

### No Messages Being Consumed

If the consumer starts but doesn't process messages:
- Verify topics exist: `make topics`
- Check init-kafka logs: `make logs-init`
- View messages in Kafka UI: http://localhost:8080
- Check consumer group status:
  ```bash
  docker exec kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --group order-processor-group \
    --describe
  ```

## Learning Points

This example demonstrates:
- How to configure the resilient consumer with JSON configuration
- How to implement a custom message handler
- How to return different result types (SuccessResult, RetryableResult, ErrorResult)
- How to use dependency injection with the library
- How to handle graceful shutdown
- How retry logic automatically moves messages through retry topics
- How non-retryable errors go directly to the error topic
- Using Kafka in KRaft mode (without Zookeeper)
