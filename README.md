# ResilientQ.Consumer.Kafka

A resilient Kafka consumer library for .NET with automatic retry logic, dead letter queue pattern, and support for both JSON and Avro messages.

## Features

- ✅ **Auto-start via IHostedService** - Consumer starts automatically with your application
- ✅ Resilient Kafka consumer with automatic retry logic
- ✅ JSON file configuration
- ✅ Configurable retry attempts and delays
- ✅ Dead Letter Queue (DLQ) pattern for failed messages
- ✅ Schema Registry support (Confluent Schema Registry)
- ✅ **Support for both JSON and Avro messages** (automatic detection and deserialization)
- ✅ Message keys support for proper partitioning
- ✅ Dependency injection support
- ✅ **Compatible with ASP.NET Core and Console apps**
- ✅ Docker Compose setup with KRaft mode (no Zookeeper)
- ✅ Multiple concurrent consumers
- ✅ Comprehensive logging
- ✅ Graceful shutdown handling

## Quick Start

```csharp
// 1. Install the package
// dotnet add package ResilientQ.Consumer.Kafka

// 2. Create your message handler
public class OrderMessageHandler : IMessageHandler<OrderMessage>
{
    public async Task<ConsumerResult> HandleAsync(OrderMessage message, ...)
    {
        // Process your message
        return new SuccessResult();
    }
}

// 3. Register and run
var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        services.AddResilientKafkaConsumer<OrderMessage, OrderMessageHandler>(
            context.Configuration.GetSection("KafkaConsumer"));
    })
    .Build();

await host.RunAsync(); // Consumer starts automatically!
```

## Installation

### Requirements

- **.NET 10.0** or higher
- Docker (for running Kafka locally)

### As a NuGet Package

```bash
dotnet add package ResilientQ.Consumer.Kafka
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
      "RetryTopics": [
        {
          "TopicName": "orders.retry.1",
          "Delay": "00:00:30",
          "RetryAttempts": 3
        },
        {
          "TopicName": "orders.retry.2",
          "Delay": "00:05:00",
          "RetryAttempts": 2
        },
        {
          "TopicName": "orders.retry.3",
          "Delay": "00:30:00",
          "RetryAttempts": 1
        }
      ]
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
- **RetryPolicy.RetryTopics**: Array of retry topics with configurable delays and attempts
  - **TopicName**: Name of the retry topic (required)
  - **Delay**: Time to wait before retrying in "HH:mm:ss" format (required)
  - **RetryAttempts**: Number of attempts in this topic before moving to next (default: 1)
  - **GroupId**: Custom group ID for this retry topic (optional, defaults to main GroupId)

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
using ResilientQ.Consumer.Kafka.Interfaces;
using ResilientQ.Consumer.Kafka.Models;

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
using ResilientQ.Consumer.Kafka.Extensions;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        // Register the resilient Kafka consumer
        services.AddResilientKafkaConsumer<OrderMessage, OrderMessageHandler>(
            context.Configuration.GetSection("KafkaConsumer"));
    })
    .Build();

// Consumer starts automatically! Just run the host
await host.RunAsync();
```

**That's it!** The consumer starts automatically via `IHostedService`. No need to manually call `StartAsync()` or handle shutdown logic.

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

### Building NuGet Package

```bash
# Build and create NuGet package
dotnet pack --configuration Release

# Output will be in:
# src/ResilientQ.Consumer.Kafka/bin/Release/ResilientQ.Consumer.Kafka.1.2.0.nupkg
```

## Project Structure

```
ResilientQ.Consumer.Kafka/
├── src/
│   └── Kafka.Consumer.Resilient/      # Main library (NuGet package)
├── example/
│   ├── Example/                        # Consumer example
│   ├── Kafka.Consumer.Resilient.AvroProducer/  # Avro producer example
│   ├── docker-compose.yml              # Kafka infrastructure
│   ├── Makefile                        # Development commands
│   └── Documentation (see below)
├── tests/
│   └── (Test projects)
└── ResilientQ.Consumer.sln        # Solution file
```

## Documentation

- [Example README](example/README.md) - Example application guide with Makefile commands
- [Changelog](CHANGELOG.md) - Version history and release notes


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

- [GitHub Repository](https://github.com/cesarl/ResilientQ.Consumer.Kafka)
- [Confluent Kafka Documentation](https://docs.confluent.io/)
- [Apache Avro Documentation](https://avro.apache.org/docs/)
