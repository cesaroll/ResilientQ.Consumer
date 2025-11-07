#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Real Avro Message Producer ===${NC}"
echo ""

# Check if Kafka is running
if ! docker ps | grep -q kafka; then
    echo "❌ Kafka is not running!"
    echo "Please start it with: docker-compose up -d"
    exit 1
fi

# Check if Schema Registry is running
if ! docker ps | grep -q schema-registry; then
    echo "❌ Schema Registry is not running!"
    echo "Please start it with: docker-compose up -d"
    exit 1
fi

echo -e "${GREEN}✓ Kafka is running${NC}"
echo -e "${GREEN}✓ Schema Registry is running${NC}"
echo ""

# Number of messages to produce
COUNT=${1:-5}

echo "Producing $COUNT REAL Avro messages (binary format) to 'orders' topic..."
echo ""

# Use the C# Avro producer
dotnet run --project Kafka.Consumer.Resilient.AvroProducer/Kafka.Consumer.Resilient.AvroProducer.csproj $COUNT

echo ""
echo "To verify messages:"
echo "  View keys:      kcat -b localhost:9092 -t orders -C -f 'Key: %k | Offset: %o\n' -e -o beginning"
echo "  Avro consumer:  docker exec schema-registry kafka-avro-console-consumer --bootstrap-server kafka:29092 --topic orders --from-beginning --property schema.registry.url=http://localhost:8081"
echo "  Schema:         curl http://localhost:8081/subjects/orders-value/versions/latest"
echo "  Kafka UI:       http://localhost:8080"
echo ""
echo -e "${YELLOW}✨ These are REAL Avro messages (binary, not JSON)!${NC}"

