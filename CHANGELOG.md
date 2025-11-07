# Changelog

All notable changes to the Ces.Kafka.Consumer.Resilient project will be documented in this file.

## [Unreleased]

### Added
- Initial release of Ces.Kafka.Consumer.Resilient library
- Resilient Kafka consumer with automatic retry logic
- Support for JSON and Avro message formats
- Configurable retry policies with custom delays and attempts
- Multiple concurrent consumers support
- Dead letter queue (error topic) for failed messages
- Three result types: SuccessResult, RetryableResult, ErrorResult
- Automatic topic creation via init-kafka service
- Example console application with complete implementation
- Makefile for easy development operations
- Docker Compose setup with Kafka KRaft mode
- Kafka UI integration for monitoring
