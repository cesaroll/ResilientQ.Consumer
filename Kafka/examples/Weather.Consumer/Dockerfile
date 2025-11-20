# Build stage
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /

# Copy NuGet.config for BaGet access
COPY ["NuGet.config", "./"]

# Copy project files
COPY ["src/Weather.Consumer/Weather.Consumer.csproj", "Weather.Consumer/"]
COPY ["src/Weather.Contracts/Weather.Contracts.csproj", "Weather.Contracts/"]

# Restore dependencies
RUN dotnet restore "Weather.Consumer/Weather.Consumer.csproj"

# Copy source code
COPY ["src/Weather.Consumer/", "Weather.Consumer/"]
COPY ["src/Weather.Contracts/", "Weather.Contracts/"]

# Build the application
WORKDIR "/Weather.Consumer"
RUN dotnet build "Weather.Consumer.csproj" -c Release -o /app/build

# Publish stage
FROM build AS publish
RUN dotnet publish "Weather.Consumer.csproj" -c Release -o /app/publish /p:UseAppHost=false

# Runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app

# Copy published output
COPY --from=publish /app/publish .

# Expose port
EXPOSE 8080
EXPOSE 8081

# Set environment variables
ENV ASPNETCORE_URLS=http://+:8080
ENV ASPNETCORE_ENVIRONMENT=Development

# Run the application
ENTRYPOINT ["dotnet", "Weather.Consumer.dll"]
