#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Kafka Test Message Producer ===${NC}"
echo ""

# Check if Kafka is running
if ! docker ps | grep -q kafka; then
    echo "❌ Kafka is not running!"
    echo "Please start it with: docker-compose up -d"
    exit 1
fi

echo -e "${GREEN}✓ Kafka is running${NC}"
echo ""

# Number of messages to produce
COUNT=${1:-5}

echo "Producing $COUNT test messages to 'orders' topic..."
echo ""

for i in $(seq 1 $COUNT); do
  ORDER_ID=$(printf "ORD-%03d" $i)
  CUSTOMER_ID=$(printf "CUST-%03d" $((RANDOM % 100 + 1)))
  AMOUNT=$(awk -v min=10 -v max=500 'BEGIN{srand(); print int(min+rand()*(max-min)*100)/100}')
  DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  # Use OrderId as the message key for proper partitioning
  KEY="$ORDER_ID"
  VALUE="{\"OrderId\":\"$ORDER_ID\",\"CustomerId\":\"$CUSTOMER_ID\",\"Amount\":$AMOUNT,\"OrderDate\":\"$DATE\",\"Status\":\"Pending\"}"

  # Format: KEY:VALUE
  MESSAGE="$KEY:$VALUE"

  echo "$MESSAGE" | docker exec -i kafka kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic orders \
    --property "parse.key=true" \
    --property "key.separator=:" 2>/dev/null

  echo -e "${GREEN}✓${NC} Produced: $ORDER_ID (Key: $KEY, Customer: $CUSTOMER_ID, Amount: \$$AMOUNT)"
  # sleep 0.3
done

echo ""
echo -e "${GREEN}✓ All $COUNT messages produced successfully!${NC}"
echo ""
echo "To monitor messages:"
echo "  Main topic:    docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning --property print.key=true --property key.separator=:"
echo "  Error topic:   docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders.error --from-beginning --property print.key=true --property key.separator=:"
echo "  Kafka UI:      http://localhost:8080"

