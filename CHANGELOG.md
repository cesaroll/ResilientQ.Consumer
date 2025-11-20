# Changelog

All notable changes to the ResilientQ.Consumer.Kafka project will be documented in this file.

## [1.7.0] - 2025-11-14

### Changed
- **Fixes and refactoring
- Renaming for better maintainability

## [1.6.0] - 2025-11-14

### Changed
- **Fixes and refactoring
- Refactored internal logic for better maintainability
- Improved retry handling
- Fix Retry groupId assignment logic

## [1.2.0] - 2025-11-14

### Changed
- **Upgraded to .NET 10**: All projects now target .NET 10
- Updated all `Microsoft.Extensions.*` packages to version 10.0.0
- Updated `Confluent.Kafka` to version 2.12.0 (library and tests)
- Updated `Confluent.SchemaRegistry` to version 2.12.0
- Updated Aspire packages to version 13.0.0 for testing infrastructure
- Updated test packages:
  - `FluentAssertions` to 7.2.0
  - `xunit` to 2.9.3
  - `xunit.runner.visualstudio` to 3.1.5
  - `Microsoft.NET.Test.Sdk` to 18.0.1
  - `coverlet.collector` to 6.0.4
  - `Apache.Avro` to 1.12.1
- Updated README with NuGet package management information
- Uses latest .NET runtime features and performance improvements
- Removed `global.json` SDK version lock for better flexibility

## [1.1.0] - 2025-11-14

### Added
- **Auto-start functionality**: Consumer now starts automatically via `IHostedService`
- `ResilientKafkaConsumerHostedService` for automatic lifecycle management
- No need to manually call `StartAsync()` - consumer starts when host starts
- Automatic graceful shutdown handling

### Changed
- **Migrated to .NET 9**: Changed from .NET 10 RC to .NET 9 (stable)
- Updated all `Microsoft.Extensions.*` packages to version 9.0.0
- Simplified client code - reduced example from ~80 lines to ~16 lines
- Example application now uses `Host.RunAsync()` for cleaner lifecycle management
- Consumer lifecycle is now managed by `IHostedService` instead of manual calls

### Improved
- Better compatibility with ASP.NET Core applications
- Standard .NET hosting patterns integration
- Cleaner separation of concerns between library and client code

## [1.0.0] - 2024-11-14

### Added
- Initial release of ResilientQ.Consumer.Kafka library
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
