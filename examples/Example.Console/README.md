# Kafka.Consumer.Resilient Example

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

1. **Kafka** running on `localhost:9092` (in KRaft mode - no Zookeeper needed!)
2. **Docker** and **Docker Compose**

### Quick Start with Docker Compose

```bash
# Start all services (Kafka, Schema Registry, Kafka UI)
make up

# Or using docker-compose directly
docker-compose up -d
```

This will automatically:
- ✅ Start Kafka in KRaft mode
- ✅ Create all required topics
- ✅ Start Schema Registry
- ✅ Start Kafka UI (http://localhost:8080)

## Running the Example

### Using Makefile (Recommended)

1. **Start infrastructure**:
   ```bash
   make up
   ```

2. **Run the example**:
   ```bash
   make run
   ```

3. **Produce test messages** (in another terminal):
   ```bash
   make produce
   # Or produce custom number: make produce N=20
   ```

### Using dotnet directly

1. **Ensure Kafka is running** on localhost:9092

2. **Run the example**:
   ```bash
   dotnet run --project Example
   ```

3. **Produce test messages** to the `orders` topic:

   **Using Makefile** (easiest):
   ```bash
   make produce        # Produces 10 messages
   make produce N=50   # Produces 50 messages
   ```

   **Using the helper script**:
   ```bash
   ./produce-test-messages.sh 10
   ```

   **Using kafka-console-producer** (Docker):
   ```bash
   docker exec -i kafka kafka-console-producer \
     --bootstrap-server localhost:9092 \
     --topic orders
   ```

   Then type messages (one per line):
   ```json
   {"OrderId":"ORD-001","CustomerId":"CUST-123","Amount":99.99,"OrderDate":"2024-01-15T10:30:00Z","Status":"Pending"}
   {"OrderId":"ORD-002","CustomerId":"CUST-456","Amount":149.50,"OrderDate":"2024-01-15T11:00:00Z","Status":"Pending"}
   ```

   **Quick one-liner**:
   ```bash
   echo '{"OrderId":"ORD-001","CustomerId":"CUST-123","Amount":99.99,"OrderDate":"2024-01-15T10:30:00Z","Status":"Pending"}' | \
     docker exec -i kafka kafka-console-producer --bootstrap-server localhost:9092 --topic orders
   ```

## What to Observe

The example simulates three scenarios:

1. **Success (80%)**: Message is processed successfully
2. **Retryable Error (10%)**: Simulates a temporary database connection issue
   - Message will be moved to retry topics: `orders.retry.1`, `orders.retry.2`, `orders.retry.3`
   - Each retry has a 2-second delay
3. **Non-Retryable Error (10%)**: Simulates invalid data
   - Message is sent directly to error topic: `orders.error`

## Configuration

The `appsettings.json` file contains the Kafka consumer configuration:

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

## Monitoring Topics

### Using Kafka UI (Recommended)
Open http://localhost:8080 in your browser to see:
- All topics and messages
- Consumer groups and lag
- Schema Registry
- Real-time message flow

### Using Command Line

```bash
# List all topics
make topics

# Monitor main topic
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning

# Monitor retry topics
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders.retry.1 --from-beginning
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders.retry.2 --from-beginning
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders.retry.3 --from-beginning

# Monitor error topic (with headers)
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders.error --from-beginning --property print.headers=true
```

## Code Structure

- **`OrderMessage.cs`**: The message model representing an order
- **`OrderMessageHandler.cs`**: Handler that processes order messages and returns Success/Retry/Error results
- **`Program.cs`**: Application entry point that configures and starts the consumer
- **`appsettings.json`**: Configuration file for the Kafka consumer

## Makefile Commands

```bash
make help      # Show all available commands
make up        # Start all services
make down      # Stop all services
make restart   # Restart services
make clean     # Stop and remove all data
make logs      # View all logs
make topics    # List Kafka topics
make produce   # Produce test messages
make run       # Run the consumer
make build     # Build the solution
```

## Troubleshooting

### Connection Refused

If you see connection errors, ensure:
- Kafka is running on localhost:9092
- Run `make ps` to check container status
- Run `make logs-kafka` to check Kafka logs
- No firewall is blocking the connections

### No Messages Being Consumed

If the consumer starts but doesn't process messages:
- Verify topics exist: `make topics`
- Check init-kafka logs: `make logs-init`
- Check consumer group status:
  ```bash
  docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
    --group order-processor-group --describe
  ```
- View messages in Kafka UI: http://localhost:8080

## Quick Start Summary

```bash
# 1. Start infrastructure
make up

# 2. Run consumer (in one terminal)
make run

# 3. Produce messages (in another terminal)
make produce

# 4. Monitor in Kafka UI
open http://localhost:8080

# 5. Stop everything
make down
```

## Learning Points

This example demonstrates:
- ✅ How to configure the resilient consumer with JSON configuration
- ✅ How to implement a custom message handler
- ✅ How to return different result types (SuccessResult, RetryableResult, ErrorResult)
- ✅ How to use dependency injection with the library
- ✅ How to handle graceful shutdown
- ✅ How retry logic automatically moves messages through retry topics
- ✅ How non-retryable errors go directly to the error topic
- ✅ Using Kafka in KRaft mode (without Zookeeper)

