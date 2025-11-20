# Build stage
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /app

# Copy solution and project files
COPY ["ResilientQ.Consumer.Kafka.sln", "./"]
COPY ["README.md", "./"]
COPY ["src/ResilientQ.Consumer.Kafka/ResilientQ.Consumer.Kafka.csproj", "src/ResilientQ.Consumer.Kafka/"]
COPY ["examples/Example.Console/Example.Console.csproj", "examples/Example.Console/"]

# Restore dependencies
RUN dotnet restore "examples/Example.Console/Example.Console.csproj"

# Copy source code
COPY ["src/", "src/"]
COPY ["examples/Example.Console/", "examples/Example.Console/"]

# Build and publish
WORKDIR "/app/examples/Example.Console"
RUN dotnet build "Example.Console.csproj" -c Release -o /build /p:GeneratePackageOnBuild=false
RUN dotnet publish "Example.Console.csproj" -c Release -o /publish /p:UseAppHost=false /p:GeneratePackageOnBuild=false

# Runtime stage
FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime
WORKDIR /app
COPY --from=build /publish .

ENTRYPOINT ["dotnet", "Example.Console.dll"]
