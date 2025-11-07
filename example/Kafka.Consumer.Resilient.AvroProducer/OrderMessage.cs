using Avro;
using Avro.Specific;

namespace Kafka.Consumer.Resilient.AvroProducer;

/// <summary>
/// Order message model - Implements ISpecificRecord for Avro serialization
/// </summary>
public class OrderMessage : ISpecificRecord
{
    public static Schema _SCHEMA = Schema.Parse(@"{
        ""type"": ""record"",
        ""name"": ""OrderMessage"",
        ""namespace"": ""Kafka.Consumer.Resilient.Example"",
        ""fields"": [
            {""name"": ""OrderId"", ""type"": ""string""},
            {""name"": ""CustomerId"", ""type"": ""string""},
            {""name"": ""Amount"", ""type"": ""double""},
            {""name"": ""OrderDate"", ""type"": ""string""},
            {""name"": ""Status"", ""type"": ""string""}
        ]
    }");

    public Schema Schema => _SCHEMA;

    public string OrderId { get; set; } = string.Empty;
    public string CustomerId { get; set; } = string.Empty;
    public double Amount { get; set; }
    public string OrderDate { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;

    public object Get(int fieldPos)
    {
        return fieldPos switch
        {
            0 => OrderId,
            1 => CustomerId,
            2 => Amount,
            3 => OrderDate,
            4 => Status,
            _ => throw new AvroRuntimeException($"Bad index {fieldPos} in Get()")
        };
    }

    public void Put(int fieldPos, object fieldValue)
    {
        switch (fieldPos)
        {
            case 0: OrderId = (string)fieldValue; break;
            case 1: CustomerId = (string)fieldValue; break;
            case 2: Amount = (double)fieldValue; break;
            case 3: OrderDate = (string)fieldValue; break;
            case 4: Status = (string)fieldValue; break;
            default: throw new AvroRuntimeException($"Bad index {fieldPos} in Put()");
        }
    }
}

