# Build stage
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy solution file
COPY ["Kafka.Consumer.Resilient.sln", "./"]

# Copy project files
COPY ["src/Kafka.Consumer.Resilient/Kafka.Consumer.Resilient.csproj", "src/Kafka.Consumer.Resilient/"]
COPY ["example/Example/Example.csproj", "example/Example/"]

# Restore dependencies
RUN dotnet restore "example/Example/Example.csproj"

# Copy everything else
COPY ["src/", "src/"]
COPY ["example/Example/", "example/Example/"]

# Build and publish
WORKDIR "/src/example/Example"
RUN dotnet build "Example.csproj" -c Release -o /app/build
RUN dotnet publish "Example.csproj" -c Release -o /app/publish /p:UseAppHost=false

# Runtime stage
FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "Example.dll"]
