using Ces.Kafka.Consumer.Resilient.Extensions;
using Ces.Weather.Contracts.Models;
using Scalar.AspNetCore;
using Weather.Consumer.Consumers;
using Weather.Consumer.Features.WeatherForecasts;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

// Add Kafka consumers
builder.Services.AddResilientKafkaConsumer<WeatherForecast, WeatherEventsConsumer>(
    builder.Configuration.GetSection("KafkaConsumer"));

var app = builder.Build();

// Add OpenAPI/Swagger middlewares
app.MapOpenApi();
app.MapScalarApiReference();

// Configure the HTTP request pipeline.
if (app.Environment.IsProduction())
{
    app.UseHttpsRedirection();
}

// Map feature endpoints
app.MapWeatherForecastEndpoints();

app.Run();

