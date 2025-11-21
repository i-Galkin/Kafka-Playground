using OrderService.Extensions;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddApplicationConfiguration(builder.Configuration)
    .AddApplicationServices(builder.Configuration)
    .AddDatabase(builder.Configuration)
    .AddKafkaConsumers(builder.Configuration);

    // TODO: Add healthcheck
    // .AddHealthChecks().AddCheck("database", () =>
    // {
    //    return Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckResult.Healthy();
    // });

var host = builder.Build();

// host.MapHealthChecks("/health");

await host.MigrateDatabaseAsync();
await host.RunAsync();
