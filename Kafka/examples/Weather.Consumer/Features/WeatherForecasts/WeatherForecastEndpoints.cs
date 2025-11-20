using Weather.Consumer.Application.WeatherForecasts;

namespace Weather.Consumer.Features.WeatherForecasts;

public static class WeatherForecastEndpoints
{
    public static void MapWeatherForecastEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/weatherforecast")
            .WithTags("WeatherForecast");

        group.MapGet("/", GetWeatherForecast)
            .WithName("GetWeatherForecast");
    }

    private static IResult GetWeatherForecast(GetWeatherForecastHandler handler)
    {
        var query = new GetWeatherForecastQuery();
        var result = handler.Handle(query);
        return Results.Ok(result);
    }
}

