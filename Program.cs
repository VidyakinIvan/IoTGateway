using Confluent.Kafka;
using OpenTelemetry.Metrics;

var builder = WebApplication.CreateBuilder(args);

var kafkaBootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") 
    ?? "my-cluster-kafka-bootstrap.kafka:9092";

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddAspNetCoreInstrumentation()
        .AddPrometheusExporter());

builder.Services.AddSingleton<IProducer<string, byte[]>>(sp =>
{
    var config = new ProducerConfig
    {
        BootstrapServers = kafkaBootstrap,
        EnableIdempotence = true,
        Acks = Acks.All
    };
    return new ProducerBuilder<string, byte[]>(config).Build();
});

builder.Services.AddControllers();
builder.Services.AddHealthChecks();

var app = builder.Build();

app.MapPrometheusScrapingEndpoint();
app.MapControllers();
app.MapHealthChecks("/health");

app.Run();
