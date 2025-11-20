namespace Ces.Weather.Contracts.Models;

public record WeatherForecast(
    DateTime DateTime,
    string City,
    int Temperature,
    string Summary);

