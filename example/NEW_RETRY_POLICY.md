# New Retry Policy Configuration

## Overview

The retry policy has been enhanced to support multiple retry topics with individual configuration for each topic, including custom delays, retry attempts, and optional group IDs.

## Configuration Structure

```json
{
  "RetryPolicy": {
    "RetryTopics": [
      {
        "TopicName": "orders.retry.immediate",
        "Delay": "00:00:30",
        "RetryAttempts": 3,
        "GroupId": "optional-custom-group-id"
      }
    ]
  }
}
```

### Configuration Properties

#### RetryTopicConfiguration

| Property | Required | Type | Description | Example |
|----------|----------|------|-------------|---------|
| `TopicName` | ✅ Yes | string | Name of the retry topic | `"orders.retry.1"` |
| `Delay` | ✅ Yes | string | Delay in HH:mm:ss format | `"00:25:00"` (25 minutes) |
| `RetryAttempts` | ❌ No | int | Number of retries in this topic (default: 1) | `3` |
| `GroupId` | ❌ No | string | Custom consumer group ID (uses main GroupId if not provided) | `"orders-retry-1-group"` |

## How It Works

### Retry Flow

1. **Message fails** with `RetryableResult`
2. **Check current retry topic** configuration
3. **Retry in same topic** if attempts < `RetryAttempts` for that topic
4. **Move to next retry topic** if max attempts in current topic reached
5. **Send to error topic** if all retry topics exhausted

### Example Flow

Given this configuration:

```json
{
  "RetryTopics": [
    {
      "TopicName": "orders.retry.immediate",
      "Delay": "00:00:30",
      "RetryAttempts": 3
    },
    {
      "TopicName": "orders.retry.short",
      "Delay": "00:05:00",
      "RetryAttempts": 2
    },
    {
      "TopicName": "orders.retry.long",
      "Delay": "01:00:00",
      "RetryAttempts": 1
    }
  ]
}
```

**Flow:**

```
Main Topic (orders)
  ↓ Fails
orders.retry.immediate (attempt 1) → wait 30s
  ↓ Fails
orders.retry.immediate (attempt 2) → wait 30s
  ↓ Fails
orders.retry.immediate (attempt 3) → wait 30s
  ↓ Fails (max attempts in this topic)
orders.retry.short (attempt 1) → wait 5min
  ↓ Fails
orders.retry.short (attempt 2) → wait 5min
  ↓ Fails (max attempts in this topic)
orders.retry.long (attempt 1) → wait 1hr
  ↓ Fails (no more retry topics)
orders.error (Error Topic)
```

**Total retry attempts:** 6 (3 + 2 + 1)

## Delay Format

The delay must be specified in `HH:mm:ss` format:

| Example | Duration |
|---------|----------|
| `"00:00:30"` | 30 seconds |
| `"00:05:00"` | 5 minutes |
| `"00:25:00"` | 25 minutes |
| `"01:00:00"` | 1 hour |
| `"02:30:00"` | 2 hours 30 minutes |

## Custom Group IDs

You can optionally specify a different consumer group ID for each retry topic:

```json
{
  "RetryTopics": [
    {
      "TopicName": "orders.retry.immediate",
      "Delay": "00:00:30",
      "RetryAttempts": 3,
      "GroupId": "orders-immediate-retry-group"
    },
    {
      "TopicName": "orders.retry.long",
      "Delay": "01:00:00",
      "RetryAttempts": 1,
      "GroupId": "orders-long-retry-group"
    }
  ]
}
```

**Benefits:**
- Separate consumer lag tracking per retry topic
- Independent offset management
- Better monitoring and observability

## Message Headers

The library automatically tracks retry state using message headers:

| Header | Type | Description |
|--------|------|-------------|
| `retry-topic-index` | int | Index of current retry topic (0-based) |
| `retry-attempts-in-topic` | int | Number of attempts in current topic |
| `retry-total-attempts` | int | Total attempts across all topics |
| `retry-timestamp` | string | ISO 8601 timestamp of last retry |

## Backward Compatibility

The old configuration format is still supported:

```json
{
  "RetryPolicy": {
    "Delay": 2000,
    "RetryAttempts": 3
  }
}
```

This will automatically create retry topics named:
- `orders.retry.1`
- `orders.retry.2`
- `orders.retry.3`

## Migration Guide

### From Old Format

**Before:**
```json
{
  "RetryPolicy": {
    "Delay": 5000,
    "RetryAttempts": 3
  }
}
```

**After:**
```json
{
  "RetryPolicy": {
    "RetryTopics": [
      {
        "TopicName": "orders.retry.1",
        "Delay": "00:00:05",
        "RetryAttempts": 1
      },
      {
        "TopicName": "orders.retry.2",
        "Delay": "00:00:05",
        "RetryAttempts": 1
      },
      {
        "TopicName": "orders.retry.3",
        "Delay": "00:00:05",
        "RetryAttempts": 1
      }
    ]
  }
}
```

### Best Practices

1. **Start with short delays** for immediate retries:
   ```json
   {
     "TopicName": "orders.retry.immediate",
     "Delay": "00:00:10",
     "RetryAttempts": 3
   }
   ```

2. **Increase delays progressively**:
   ```json
   {
     "TopicName": "orders.retry.medium",
     "Delay": "00:05:00",
     "RetryAttempts": 2
   }
   ```

3. **Use long delays for final attempts**:
   ```json
   {
     "TopicName": "orders.retry.final",
     "Delay": "01:00:00",
     "RetryAttempts": 1
   }
   ```

4. **Fewer attempts in later topics** to avoid overwhelming the system

5. **Monitor retry topics** using dedicated consumer groups

## Example Configurations

### Quick Retry Strategy
```json
{
  "RetryTopics": [
    {
      "TopicName": "orders.retry.fast",
      "Delay": "00:00:05",
      "RetryAttempts": 5
    }
  ]
}
```

### Gradual Backoff Strategy
```json
{
  "RetryTopics": [
    {
      "TopicName": "orders.retry.immediate",
      "Delay": "00:00:30",
      "RetryAttempts": 3
    },
    {
      "TopicName": "orders.retry.short",
      "Delay": "00:05:00",
      "RetryAttempts": 2
    },
    {
      "TopicName": "orders.retry.medium",
      "Delay": "00:30:00",
      "RetryAttempts": 2
    },
    {
      "TopicName": "orders.retry.long",
      "Delay": "02:00:00",
      "RetryAttempts": 1
    }
  ]
}
```

### Aggressive Retry Strategy
```json
{
  "RetryTopics": [
    {
      "TopicName": "orders.retry.burst",
      "Delay": "00:00:01",
      "RetryAttempts": 10
    },
    {
      "TopicName": "orders.retry.final",
      "Delay": "00:10:00",
      "RetryAttempts": 3
    }
  ]
}
```

## Monitoring

Check retry metrics using Kafka consumer groups:

```bash
# View all retry consumer groups
kafka-consumer-groups --bootstrap-server localhost:9092 --list | grep retry

# Check lag for specific retry topic
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group orders-immediate-retry-group --describe
```

## Logging

The library provides detailed logging for retry operations:

```
warn: Retryable error: Database timeout. Retrying in same topic orders.retry.immediate
      (attempt 2/3 in this topic, total attempts: 2)

warn: Max attempts in current topic reached. Moving to next retry topic orders.retry.short
      (total attempts: 4)

error: All retry attempts exhausted across all topics (total: 6). Sending to error topic
```

