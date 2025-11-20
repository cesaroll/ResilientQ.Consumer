using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Logging;

namespace ResilientQ.Consumer.Kafka.Services;

/// <summary>
/// Handles message deserialization for both JSON and Avro formats
/// </summary>
/// <typeparam name="TMessage">Type of message to deserialize</typeparam>
public class MessageDeserializer<TMessage> where TMessage : class
{
    private readonly ILogger<MessageDeserializer<TMessage>> _logger;

    public MessageDeserializer(ILogger<MessageDeserializer<TMessage>> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Deserializes a message from bytes to the specified type
    /// </summary>
    public async Task<TMessage?> DeserializeAsync(
        byte[] messageBytes,
        bool isAvro,
        AvroDeserializer<TMessage>? avroDeserializer,
        CancellationToken cancellationToken)
    {
        try
        {
            if (isAvro && avroDeserializer != null)
            {
                // Deserialize Avro
                return await avroDeserializer.DeserializeAsync(
                    messageBytes,
                    false,
                    new SerializationContext(MessageComponentType.Value, string.Empty));
            }
            else
            {
                // Deserialize JSON
                var json = Encoding.UTF8.GetString(messageBytes);
                return JsonSerializer.Deserialize<TMessage>(json);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to deserialize message");
            return null;
        }
    }
}
