using ResilientQ.Consumer.Kafka.Tests.Integration.Fixtures;
using Xunit;

namespace ResilientQ.Consumer.Kafka.Tests.Integration;

/// <summary>
/// Collection definition to share Kafka infrastructure across all integration tests
/// This ensures the Kafka container is started once and reused by all test classes
/// </summary>
[CollectionDefinition("Kafka")]
public class KafkaTestCollection : ICollectionFixture<KafkaTestFixture>
{
    // This class is intentionally empty
    // It's just used to define the collection
}

