.PHONY: help unit integration all build clean restore watch coverage

# Default target
.DEFAULT_GOAL := help

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
NC := \033[0m # No Color

help:
	@echo ""
	@echo "$(BLUE)ResilientQ.Consumer.Kafka - Tests Makefile$(NC)"
	@echo ""
	@echo "$(GREEN)Available targets:$(NC)"
	@echo ""
	@echo "  $(YELLOW)make unit$(NC)              - Run unit tests only (fast, no dependencies)"
	@echo "  $(YELLOW)make integration$(NC)       - Run integration tests (Aspire starts Kafka automatically)"
	@echo "  $(YELLOW)make all$(NC)               - Run all tests (unit + integration)"
	@echo ""
	@echo "  $(YELLOW)make build$(NC)             - Build all test projects"
	@echo "  $(YELLOW)make restore$(NC)           - Restore NuGet packages"
	@echo "  $(YELLOW)make clean$(NC)             - Clean build artifacts"
	@echo ""
	@echo "  $(YELLOW)make watch-unit$(NC)        - Run unit tests in watch mode"
	@echo "  $(YELLOW)make watch-integration$(NC) - Run integration tests in watch mode"
	@echo ""
	@echo "  $(YELLOW)make coverage$(NC)          - Run tests with code coverage"
	@echo ""
	@echo "$(GREEN)Quick Start:$(NC)"
	@echo "  make unit                  # Fast feedback with unit tests"
	@echo "  make integration           # Integration tests (Aspire handles Kafka automatically)"
	@echo "  make all                   # Run everything (fully independent)"
	@echo ""

# Restore NuGet packages
restore:
	@echo "$(BLUE)📦 Restoring NuGet packages...$(NC)"
	@dotnet restore ../../ResilientQ.Consumer.sln

# Build test projects
build: restore
	@echo "$(BLUE)🔨 Building test projects...$(NC)"
	@dotnet build ../../ResilientQ.Consumer.sln --no-restore

# Clean build artifacts
clean:
	@echo "$(BLUE)🧹 Cleaning build artifacts...$(NC)"
	@dotnet clean ../../ResilientQ.Consumer.sln
	@find . -type d -name "bin" -o -name "obj" | xargs rm -rf
	@echo "$(GREEN)✓ Clean complete$(NC)"

# Run unit tests
unit: build
	@echo ""
	@echo "$(BLUE)🧪 Running Unit Tests...$(NC)"
	@echo "$(YELLOW)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(NC)"
	@dotnet test ResilientQ.Consumer.Kafka.Tests.Unit/ \
		--no-build \
		--verbosity normal \
		--logger "console;verbosity=normal"
	@echo "$(YELLOW)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(NC)"
	@echo "$(GREEN)✓ Unit tests complete$(NC)"
	@echo ""

# Run integration tests
integration: build
	@echo ""
	@echo "$(BLUE)🧪 Running Integration Tests...$(NC)"
	@echo "$(YELLOW)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(NC)"
	@echo "$(BLUE)✨ Aspire will automatically start Kafka containers$(NC)"
	@echo "$(YELLOW)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(NC)"
	@dotnet test ResilientQ.Consumer.Kafka.Tests.Integration/ \
		--no-build \
		--verbosity normal \
		--logger "console;verbosity=normal"
	@echo "$(YELLOW)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(NC)"
	@echo "$(GREEN)✓ Integration tests complete$(NC)"
	@echo ""

# Run all tests
all: build
	@echo ""
	@echo "$(BLUE)🧪 Running ALL Tests (Unit + Integration)...$(NC)"
	@echo "$(YELLOW)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(NC)"
	@echo ""
	@echo "$(BLUE)📋 Phase 1: Unit Tests$(NC)"
	@echo "$(YELLOW)───────────────────────────────────────────────────────────────$(NC)"
	@dotnet test ResilientQ.Consumer.Kafka.Tests.Unit/ \
		--no-build \
		--verbosity minimal \
		--logger "console;verbosity=minimal"
	@echo ""
	@echo "$(BLUE)📋 Phase 2: Integration Tests$(NC)"
	@echo "$(YELLOW)───────────────────────────────────────────────────────────────$(NC)"
	@echo "$(BLUE)✨ Aspire will automatically start Kafka containers$(NC)"
	@echo "$(YELLOW)───────────────────────────────────────────────────────────────$(NC)"
	@dotnet test ResilientQ.Consumer.Kafka.Tests.Integration/ \
		--no-build \
		--verbosity minimal \
		--logger "console;verbosity=minimal"
	@echo ""
	@echo "$(YELLOW)━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━$(NC)"
	@echo "$(GREEN)✓ All tests complete$(NC)"
	@echo ""

# Watch mode for unit tests
watch-unit:
	@echo "$(BLUE)👀 Running unit tests in watch mode...$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop$(NC)"
	@dotnet watch test ResilientQ.Consumer.Kafka.Tests.Unit/ \
		--verbosity normal

# Watch mode for integration tests
watch-integration:
	@echo "$(BLUE)👀 Running integration tests in watch mode...$(NC)"
	@echo "$(BLUE)✨ Aspire will automatically start Kafka containers$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to stop$(NC)"
	@dotnet watch test ResilientQ.Consumer.Kafka.Tests.Integration/ \
		--verbosity normal

# Run tests with code coverage
coverage: build
	@echo "$(BLUE)📊 Running tests with code coverage...$(NC)"
	@dotnet test \
		--no-build \
		--collect:"XPlat Code Coverage" \
		--results-directory ./coverage \
		--verbosity normal
	@echo ""
	@echo "$(GREEN)✓ Coverage reports generated in ./coverage/$(NC)"
	@echo "$(YELLOW)💡 Tip: Install 'dotnet tool install -g dotnet-reportgenerator-globaltool' to generate HTML reports$(NC)"
	@echo ""

# Quick test (unit only, minimal output)
quick: build
	@echo "$(BLUE)⚡ Quick test run (unit tests only)...$(NC)"
	@dotnet test ResilientQ.Consumer.Kafka.Tests.Unit/ \
		--no-build \
		--verbosity minimal
	@echo "$(GREEN)✓ Quick test complete$(NC)"

# Verbose output for debugging
verbose: build
	@echo "$(BLUE)🔍 Running all tests with verbose output...$(NC)"
	@dotnet test \
		--no-build \
		--verbosity detailed \
		--logger "console;verbosity=detailed"

# List all tests without running them
list:
	@echo "$(BLUE)📋 Listing all tests...$(NC)"
	@echo ""
	@echo "$(GREEN)Unit Tests:$(NC)"
	@dotnet test ResilientQ.Consumer.Kafka.Tests.Unit/ --list-tests --verbosity quiet
	@echo ""
	@echo "$(GREEN)Integration Tests:$(NC)"
	@dotnet test ResilientQ.Consumer.Kafka.Tests.Integration/ --list-tests --verbosity quiet

# Run specific test by name
# Usage: make test-filter FILTER="TestName"
test-filter: build
	@echo "$(BLUE)🔍 Running tests matching filter: $(FILTER)$(NC)"
	@dotnet test \
		--no-build \
		--filter "FullyQualifiedName~$(FILTER)" \
		--verbosity normal

# CI/CD target - runs all tests and generates reports
ci: restore build all coverage
	@echo "$(GREEN)✓ CI pipeline complete$(NC)"

