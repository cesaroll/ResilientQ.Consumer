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

The easiest way to run this example is using the Makefile commands:

```bash
# 1. Start Kafka infrastructure
make up

# 2. Run the consumer (in one terminal)
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
- `make up` - Start all services (Kafka, Schema Registry, Kafka UI)
- `make down` - Stop all services
- `make restart` - Restart all services
- `make clean` - Stop services and remove volumes (fresh start)
- `make ps` - Show running containers

### Logs
- `make logs` - Show logs from all services
- `make logs-kafka` - Show Kafka logs
- `make logs-init` - Show init-kafka logs (topic creation)

### Kafka Operations
- `make topics` - List all Kafka topics
- `make produce` - Produce 10 test messages
- `make produce N=20` - Produce N test messages

### Application
- `make run` - Run the example consumer application
- `make build` - Build the solution
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

The consumer is configured via `Kafka.Consumer.Resilient.Example/appsettings.json`:

```json
{
  "KafkaConsumer": {
    "TopicName": "orders",
    "ConsumerNumber": 2,
    "GroupId": "order-processor-group",
    "SchemaRegistryUrl": "",
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

## Infrastructure Services

When you run `make up`, the following services are started:

- **Kafka** on `localhost:9092` (KRaft mode - no Zookeeper needed)
- **Schema Registry** on `localhost:8081`
- **Kafka UI** on `http://localhost:8080`

Topics are automatically created:
- `orders` (main topic)
- `orders.retry.1`, `orders.retry.2`, `orders.retry.3` (retry topics)
- `orders.error` (dead letter queue)

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
├── Kafka.Consumer.Resilient.Example/
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
