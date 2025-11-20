#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Avro Message Producer (via REST Proxy) ===${NC}"
echo ""

# Check if Kafka REST Proxy is running
if ! docker ps | grep -q kafka-rest; then
    echo "❌ Kafka REST Proxy is not running!"
    echo "Please start it with: make up"
    exit 1
fi

echo -e "${GREEN}✓ Kafka REST Proxy is running${NC}"
echo ""

# Number of messages to produce
COUNT=${1:-10}

# Avro schemas (inline, minified)
KEY_SCHEMA='{"type":"string"}'
VALUE_SCHEMA='{"type":"record","name":"OrderMessage","namespace":"Example","fields":[{"name":"OrderId","type":"string"},{"name":"CustomerId","type":"string"},{"name":"Amount","type":"double"},{"name":"OrderDate","type":"string"},{"name":"Status","type":"string"}]}'

echo "Producing $COUNT Avro messages to 'orders' topic via REST Proxy..."
echo ""

# Produce messages
for i in $(seq 1 $COUNT); do
    ORDER_ID=$(printf "ORD-%03d" $i)
    CUSTOMER_ID=$(printf "CUST-%03d" $((100 + RANDOM % 900)))
    AMOUNT=$(awk -v min=10 -v max=500 'BEGIN{srand(); printf "%.2f", min+rand()*(max-min)}')
    DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    # Cross-platform alternative to shuf
    STATUSES=("Pending" "Processing" "Shipped" "Delivered")
    STATUS=${STATUSES[$((RANDOM % 4))]}

    # Escape the schemas for JSON (replace " with \")
    ESCAPED_KEY_SCHEMA=$(echo "$KEY_SCHEMA" | sed 's/"/\\"/g')
    ESCAPED_VALUE_SCHEMA=$(echo "$VALUE_SCHEMA" | sed 's/"/\\"/g')

    # Create JSON payload with proper escaping (includes key)
    JSON_PAYLOAD=$(cat <<EOF
{
  "key_schema": "$ESCAPED_KEY_SCHEMA",
  "value_schema": "$ESCAPED_VALUE_SCHEMA",
  "records": [{
    "key": "$CUSTOMER_ID",
    "value": {
      "OrderId": "$ORDER_ID",
      "CustomerId": "$CUSTOMER_ID",
      "Amount": $AMOUNT,
      "OrderDate": "$DATE",
      "Status": "$STATUS"
    }
  }]
}
EOF
)

    RESPONSE=$(curl -s -X POST http://localhost:8082/topics/orders \
      -H "Content-Type: application/vnd.kafka.avro.v2+json" \
      -H "Accept: application/vnd.kafka.v2+json" \
      -d "$JSON_PAYLOAD")

    # Check if successful
    if echo "$RESPONSE" | grep -q "offsets"; then
        echo -e "${GREEN}✓${NC} Produced: $ORDER_ID | Customer: $CUSTOMER_ID | Amount: \$$AMOUNT | Status: $STATUS"
    else
        echo -e "❌ Failed to produce message $i"
        echo "   Response: $RESPONSE"
    fi
done

echo ""
echo -e "${GREEN}✓ Produced $COUNT Avro messages successfully!${NC}"
echo ""
echo "To verify messages:"
echo "  Kafka UI:       http://localhost:8080"
echo "  Console:        docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning"
echo "  With Avro:      docker exec schema-registry kafka-avro-console-consumer --bootstrap-server kafka:29092 --topic orders --from-beginning --property schema.registry.url=http://localhost:8081"
echo "  Schema:         curl http://localhost:8081/subjects/orders-value/versions/latest | jq"
echo ""
echo -e "${YELLOW}✨ These are REAL Avro messages produced via REST Proxy!${NC}"
