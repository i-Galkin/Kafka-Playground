using OrderService;
using OrderService.Extensions;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddApplicationConfiguration(builder.Configuration)
    .AddApplicationServices(builder.Configuration)
    .AddDatabase(builder.Configuration)
    .AddKafkaConsumers(builder.Configuration);

builder.Services.AddHostedService<Worker>();

var host = builder.Build();

await host.MigrateDatabaseAsync();
await host.RunAsync();
