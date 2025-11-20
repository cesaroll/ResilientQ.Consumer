using Ces.Weather.Contracts.Models;

namespace Weather.Consumer.Application.WeatherForecasts;

public record GetWeatherForecastQuery;

public class GetWeatherForecastHandler
{
    private static readonly string[] Cities = new[]
    {
        "New York", "London", "Tokyo", "Sydney", "Paris"
    };

    private readonly Random _random = new();

    public IEnumerable<WeatherForecast> Handle(GetWeatherForecastQuery query)
    {
        var forecasts = Enumerable.Range(1, 5).Select(index =>
        {
            var temperature = _random.Next(-20, 45);
            return new WeatherForecast(
                DateTime: DateTime.UtcNow.AddDays(index),
                City: Cities[_random.Next(Cities.Length)],
                Temperature: temperature,
                Summary: GetSummaryFromTemperature(temperature)
            );
        })
        .ToArray();

        return forecasts;
    }

    private static string GetSummaryFromTemperature(int temperature)
    {
        return temperature switch
        {
            < -10 => "Freezing",
            < 0 => "Very Cold",
            < 5 => "Cold",
            < 10 => "Chilly",
            < 15 => "Cool",
            < 20 => "Mild",
            < 25 => "Warm",
            < 30 => "Hot",
            < 35 => "Very Hot",
            _ => "Scorching"
        };
    }
}

