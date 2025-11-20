# ResilientQ.Consumer.Kafka - Tests

This directory contains comprehensive unit and integration tests for the ResilientQ.Consumer.Kafka library.

## 📁 Structure

```
tests/
├── ResilientQ.Consumer.Kafka.Tests.Unit/          # Unit tests
│   ├── RetryTopicNamingStrategyTests.cs
│   ├── RetryTopicConfigurationTests.cs
│   ├── ConfigurationValidationTests.cs
│   └── RetryMetadataTests.cs
│
├── ResilientQ.Consumer.Kafka.Tests.Integration/   # Integration tests with real Kafka
│   ├── Fixtures/
│   │   └── KafkaTestFixture.cs                   # Aspire-based Kafka setup
│   ├── Helpers/
│   │   └── KafkaHelpers.cs                       # Kafka test utilities
│   ├── TestModels/
│   │   ├── TestMessage.cs                        # Test message model
│   │   └── TestMessageHandlers.cs                # Test handlers
│   ├── SuccessFlowIntegrationTests.cs
│   ├── RetryPolicyIntegrationTests.cs
│   ├── ErrorRoutingIntegrationTests.cs
│   └── MultipleConsumersIntegrationTests.cs
│
└── ResilientQ.Consumer.Kafka.Tests.AppHost/       # Aspire AppHost for infrastructure
    └── Program.cs
```

## 🧪 Test Categories

### Unit Tests

Fast, isolated tests that don't require external dependencies:

- **RetryTopicNamingStrategyTests**: Tests for topic naming conventions
- **RetryTopicConfigurationTests**: Tests for delay parsing and configuration
- **ConfigurationValidationTests**: Tests for configuration validation logic
- **RetryMetadataTests**: Tests for retry metadata tracking

### Integration Tests

Tests that run against real Kafka infrastructure using .NET Aspire:

- **SuccessFlowIntegrationTests**: Tests for successful message processing
- **RetryPolicyIntegrationTests**: Tests for retry logic and metadata tracking
- **ErrorRoutingIntegrationTests**: Tests for error handling and routing
- **MultipleConsumersIntegrationTests**: Tests for concurrent consumers

## 🚀 Running Tests

### Prerequisites

- .NET 10.0 SDK
- Docker (for integration tests)

### Using the Makefile (Recommended)

The `tests/` directory includes a Makefile for easier test execution:

```bash
cd tests

# Show all available commands
make help

# Run unit tests only (fast, no dependencies)
make unit

# Run integration tests only (requires Kafka running)
make integration

# Run all tests (unit + integration)
make all

# Build test projects
make build

# Run tests in watch mode
make watch-unit
make watch-integration

# Run with code coverage
make coverage

# Quick test (unit only, minimal output)
make quick
```

### Using dotnet CLI Directly

From the repository root:

```bash
# Run all tests
dotnet test

# Run only unit tests
dotnet test tests/ResilientQ.Consumer.Kafka.Tests.Unit/

# Run only integration tests
dotnet test tests/ResilientQ.Consumer.Kafka.Tests.Integration/

# Run specific test
dotnet test --filter "FullyQualifiedName~RetryableMessage_ShouldRetryInSameTopic_BeforeMovingToNext"

# Run with verbose output
dotnet test --logger "console;verbosity=detailed"
```

## 🏗️ Integration Tests with Aspire

The integration tests are **fully independent** and use .NET Aspire to orchestrate Kafka infrastructure automatically:

### How It Works

1. **Aspire AppHost**: Defines Kafka and Schema Registry containers
2. **KafkaTestFixture**: Starts infrastructure automatically when tests begin
3. **Test Collection**: Shares fixture across all integration tests (starts containers once)
4. **Topic Management**: Each test creates unique topics and cleans up after itself
5. **Auto Cleanup**: Containers are stopped and removed when tests complete

### Advantages

- ✅ **Fully Independent**: No manual setup required, tests start their own Kafka
- ✅ **Real Kafka**: Tests run against actual Kafka containers, not mocks
- ✅ **Automatic Lifecycle**: Infrastructure starts/stops automatically
- ✅ **Reproducible**: Same behavior everywhere (local, CI/CD, colleague's machine)
- ✅ **Clean**: Containers and topics cleaned up after tests
- ✅ **Observable**: Includes Kafka UI for monitoring during test runs

### What Aspire Does

When you run `make integration`, Aspire automatically:
1. Pulls Docker images if needed (Kafka, Schema Registry, Kafka UI)
2. Starts containers with proper configuration
3. Waits for services to be ready
4. Provides connection strings to tests
5. Stops and removes containers when done

**No manual `docker-compose up` needed!** ✨

## 📊 Test Coverage

The test suite covers:

- ✅ Configuration loading and validation
- ✅ Retry topic naming strategies
- ✅ Delay parsing (HH:mm:ss format)
- ✅ Retry metadata tracking
- ✅ Successful message processing
- ✅ Retryable error handling
- ✅ Non-retryable error handling
- ✅ Multi-topic retry flow
- ✅ Multiple retry attempts per topic
- ✅ Moving between retry topics
- ✅ Error topic routing
- ✅ Multiple concurrent consumers
- ✅ Message distribution across consumers

## 🐛 Debugging Tests

### View Kafka Logs

```bash
docker logs <kafka-container-id>
```

### View Test Logs

Run tests with detailed logging:

```bash
dotnet test --logger "console;verbosity=detailed"
```

### Inspect Kafka Topics

While tests are running, you can inspect topics:

```bash
docker exec -it <kafka-container-id> kafka-topics --bootstrap-server localhost:9092 --list
```

### Consume Messages from Test Topics

```bash
docker exec -it <kafka-container-id> kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test-orders-12345678 \
  --from-beginning
```

## 🔧 Troubleshooting

### Tests Fail with "Cannot connect to Kafka"

1. Ensure Docker is running
2. Check if Aspire containers are started:
   ```bash
   docker ps | grep kafka
   ```
3. Increase timeout in `KafkaTestFixture.InitializeAsync()`

### Tests Are Slow

- Integration tests require Kafka startup (~10s)
- Use `[Trait("Category", "Integration")]` to separate fast/slow tests
- Run unit tests only for quick feedback

### Topics Already Exist

- Tests use unique topic names with GUID suffixes
- If cleanup fails, manually delete topics:
  ```bash
  docker exec -it <kafka-container-id> kafka-topics \
    --bootstrap-server localhost:9092 \
    --delete --topic test-*
  ```

## 📈 CI/CD Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Setup .NET
        uses: actions/setup-dotnet@v3
        with:
          dotnet-version: '10.0.x'

      - name: Install Aspire workload
        run: dotnet workload install aspire

      - name: Restore dependencies
        run: dotnet restore

      - name: Build
        run: dotnet build --no-restore

      - name: Run tests
        run: dotnet test --no-build --verbosity normal
```

## 📝 Writing New Tests

### Unit Test Example

```csharp
public class MyNewTests
{
    [Fact]
    public void MyTest_ShouldDoSomething()
    {
        // Arrange
        var sut = new MyClass();

        // Act
        var result = sut.DoSomething();

        // Assert
        result.Should().Be(expectedValue);
    }
}
```

### Integration Test Example

```csharp
[Collection("Kafka")]
public class MyIntegrationTests : IAsyncLifetime
{
    private readonly KafkaTestFixture _fixture;
    private readonly string _testId = Guid.NewGuid().ToString("N")[..8];
    private readonly List<string> _topicsToCleanup = new();

    public MyIntegrationTests(KafkaTestFixture fixture)
    {
        _fixture = fixture;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        await KafkaHelpers.DeleteTopicsAsync(_fixture.KafkaBootstrapServers, _topicsToCleanup.ToArray());
    }

    [Fact]
    public async Task MyTest_ShouldProcessMessage()
    {
        // Arrange
        var topic = $"test-topic-{_testId}";
        _topicsToCleanup.Add(topic);
        await KafkaHelpers.CreateTopicsAsync(_fixture.KafkaBootstrapServers, topic);

        // Act & Assert
        // ... your test code
    }
}
```

## 🎯 Best Practices

1. **Unique Test Data**: Use GUIDs in topic names to avoid conflicts
2. **Cleanup**: Always clean up topics in `DisposeAsync()`
3. **Timeouts**: Use appropriate timeouts for async operations
4. **Isolation**: Each test should be independent
5. **Descriptive Names**: Use clear, descriptive test method names
6. **Arrange-Act-Assert**: Follow AAA pattern consistently

## 📚 References

- [xUnit Documentation](https://xunit.net/)
- [FluentAssertions Documentation](https://fluentassertions.com/)
- [.NET Aspire Documentation](https://learn.microsoft.com/en-us/dotnet/aspire/)
- [Confluent.Kafka Documentation](https://docs.confluent.io/kafka-clients/dotnet/current/overview.html)

