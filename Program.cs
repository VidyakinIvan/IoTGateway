using Confluent.Kafka;
var builder = WebApplication.CreateBuilder(args);

var kafkaBootstrap = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") 
    ?? "my-cluster-kafka-bootstrap.kafka:9092";

builder.Services.AddSingleton<IProducer<string, byte[]>>(sp =>
{
    var config = new ProducerConfig
    {
        BootstrapServers = kafkaBootstrap,
        EnableIdempotence = true,
        TransactionalId = "gateway-tx-1",
        Acks = Acks.All
    };
    var producer = new ProducerBuilder<string, byte[]>(config).Build();
    producer.InitTransactions(TimeSpan.FromSeconds(10));
    return producer;
});

builder.Services.AddControllers();
builder.Services.AddHealthChecks();

var app = builder.Build();

app.MapControllers();
app.MapHealthChecks("/health");

app.Run();
