# Ces.Kafka.Consumer.Resilient

A resilient Kafka consumer library for .NET with automatic retry logic, dead letter queue pattern, and support for both JSON and Avro messages.

## Features

- ✅ Resilient Kafka consumer with automatic retry logic
- ✅ JSON file configuration
- ✅ Configurable retry attempts and delays
- ✅ Dead Letter Queue (DLQ) pattern for failed messages
- ✅ Schema Registry support (Confluent Schema Registry)
- ✅ **Support for both JSON and Avro messages** (automatic detection and deserialization)
- ✅ Message keys support for proper partitioning
- ✅ Dependency injection support
- ✅ Docker Compose setup with KRaft mode (no Zookeeper)
- ✅ Multiple concurrent consumers
- ✅ Comprehensive logging

## Quick Start

## Installation

### As a NuGet Package

```bash
dotnet add package Ces.Kafka.Consumer.Resilient
```


## Configuration

Configure your consumer in `appsettings.json`:

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

### Configuration Options

- **TopicName**: Main Kafka topic to consume from
- **ConsumerNumber**: Number of concurrent consumers (default: 1)
- **GroupId**: Kafka consumer group ID
- **SchemaRegistryUrl**: Schema Registry URL (leave empty for JSON-only mode)
- **BootstrapServers**: Kafka broker addresses
- **Error.TopicName**: Dead letter queue topic for failed messages
- **RetryPolicy.Delay**: Delay between retries in milliseconds
- **RetryPolicy.RetryAttempts**: Maximum number of retry attempts

## Usage

### 1. Define Your Message Model

```csharp
public class OrderMessage
{
    public string OrderId { get; set; } = string.Empty;
    public string CustomerId { get; set; } = string.Empty;
    public double Amount { get; set; }
    public string OrderDate { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
}
```

### 2. Implement a Message Handler

```csharp
using Ces.Kafka.Consumer.Resilient.Interfaces;
using Ces.Kafka.Consumer.Resilient.Models;

public class OrderMessageHandler : IMessageHandler<OrderMessage>
{
    private readonly ILogger<OrderMessageHandler> _logger;

    public OrderMessageHandler(ILogger<OrderMessageHandler> logger)
    {
        _logger = logger;
    }

    public async Task<ConsumerResult> HandleAsync(
        OrderMessage message,
        MessageMetadata metadata,
        CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation(
                "Processing order {OrderId} for customer {CustomerId}, amount: ${Amount}",
                message.OrderId,
                message.CustomerId,
                message.Amount);

            // Your business logic here
            await ProcessOrderAsync(message, cancellationToken);

            return new SuccessResult();
        }
        catch (TemporaryException ex)
        {
            // Retryable errors (e.g., database timeout, network issues)
            return new RetryableResult($"Temporary error: {ex.Message}", ex);
        }
        catch (Exception ex)
        {
            // Non-retryable errors (goes directly to error topic)
            return new ErrorResult($"Permanent error: {ex.Message}", ex);
        }
    }

    private async Task ProcessOrderAsync(OrderMessage message, CancellationToken cancellationToken)
    {
        // Your processing logic
        await Task.Delay(100, cancellationToken);
    }
}
```

### 3. Register and Start the Consumer

```csharp
using Ces.Kafka.Consumer.Resilient.Extensions;
using Ces.Kafka.Consumer.Resilient.Interfaces;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        // Register the resilient Kafka consumer
        services.AddResilientKafkaConsumer<OrderMessage, OrderMessageHandler>(
            context.Configuration.GetSection("KafkaConsumer"));
    })
    .Build();

// Get the consumer
var consumer = host.Services.GetRequiredService<IResilientKafkaConsumer<OrderMessage>>();

// Start consuming
await consumer.StartAsync(CancellationToken.None);
```

## Return Types

The library uses three return types to control message flow:

### SuccessResult
Message processed successfully. Consumer commits the offset and continues.

```csharp
return new SuccessResult("Order processed successfully");
```

### RetryableResult
Temporary failure. Message will be sent to retry topic.

```csharp
return new RetryableResult("Database timeout", exception);
```

### ErrorResult
Permanent failure. Message sent directly to error topic (DLQ).

```csharp
return new ErrorResult("Invalid data format", exception);
```

## Message Flow

```
Main Topic (orders)
    ↓
Consumer processes message
    ↓
    ├─→ SuccessResult → Commit offset → Continue
    ├─→ RetryableResult → Send to orders.retry.1
    │       ↓
    │   Retry 1 fails → Send to orders.retry.2
    │       ↓
    │   Retry 2 fails → Send to orders.retry.3
    │       ↓
    │   Retry 3 fails → Send to orders.error (Max retries exceeded)
    │
    └─→ ErrorResult → Send to orders.error (Immediate)
```

## Working with Avro

The library automatically handles both JSON and Avro messages. See [AVRO_GUIDE.md](example/AVRO_GUIDE.md) for detailed information.

### Quick Avro Setup

1. **Enable Schema Registry** in `appsettings.json`:
   ```json
   {
     "SchemaRegistryUrl": "http://localhost:8081"
   }
   ```

2. **Define Avro Schema** (`Schemas/OrderMessage.avsc`):
   ```json
   {
     "type": "record",
     "name": "OrderMessage",
     "namespace": "Your.Namespace.Avro",
     "fields": [
       {"name": "OrderId", "type": "string"},
       {"name": "CustomerId", "type": "string"},
       {"name": "Amount", "type": "double"},
       {"name": "OrderDate", "type": "string"},
       {"name": "Status", "type": "string"}
     ]
   }
   ```

3. **Produce Avro Messages**:
   ```bash
   cd example
   make produce-avro
   ```

4. **Your handler stays the same!** The library automatically deserializes Avro messages.


## Topic Naming Strategy

The library follows this naming convention:

- **Main topic**: `orders`
- **Retry topics**: `orders.retry.1`, `orders.retry.2`, `orders.retry.3`
- **Error topic**: `orders.error`

## Development


### Creating a NuGet Package

```bash
dotnet pack --configuration Release
```

## Project Structure

```
Ces.Kafka.Consumer.Resilient/
├── src/
│   └── Kafka.Consumer.Resilient/      # Main library (NuGet package)
├── example/
│   ├── Kafka.Consumer.Resilient.Example/    # Consumer example
│   ├── Kafka.Consumer.Resilient.AvroProducer/  # Avro producer example
│   ├── docker-compose.yml              # Kafka infrastructure
│   ├── Makefile                        # Development commands
│   └── Documentation (see below)
├── tests/
│   └── (Test projects)
└── Kafka.Consumer.Resilient.sln        # Solution file
```

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) - Technical architecture and design decisions
- [example/AVRO_GUIDE.md](example/AVRO_GUIDE.md) - Complete guide to working with Avro messages
- [example/KCAT_GUIDE.md](example/KCAT_GUIDE.md) - **kcat quick reference for testing and debugging**
- [example/KAFKA_SETUP.md](example/KAFKA_SETUP.md) - Detailed Kafka infrastructure setup
- [Example README](example/Kafka.Consumer.Resilient.Example/README.md) - Example application guide


## Troubleshooting

### Kafka Connection Issues

```bash
# Check if Kafka is running
docker ps | grep kafka

# View Kafka logs
make logs

# Restart containers
make restart
```


## Requirements

- .NET 10.0 or later
- Docker and Docker Compose (for local development)
- Kafka 7.8.0 or later (included in Docker Compose)

## License

MIT License - see LICENSE file for details


## Author

César López

## Links

- [GitHub Repository](https://github.com/cesarl/Ces.Kafka.Consumer.Resilient)
- [Confluent Kafka Documentation](https://docs.confluent.io/)
- [Apache Avro Documentation](https://avro.apache.org/docs/)
