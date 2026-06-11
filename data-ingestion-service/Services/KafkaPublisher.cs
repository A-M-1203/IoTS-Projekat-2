using Confluent.Kafka;

namespace DataIngestionService.Services;

public class KafkaPublisher : IMessagePublisher, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly string _topic;

    public KafkaPublisher(string brokers, string topic, string acks)
    {
        _topic = topic;
        var config = new ProducerConfig
        {
            BootstrapServers = brokers,
            Acks = acks switch
            {
                "0" => Acks.None,
                "all" => Acks.All,
                _ => Acks.Leader,
            },
            MessageSendMaxRetries = 3,
            EnableIdempotence = acks == "all",
        };
        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public Task EnsureConnectedAsync(CancellationToken ct) => Task.CompletedTask;

    public async Task PublishAsync(string payload, CancellationToken ct)
    {
        await _producer.ProduceAsync(_topic, new Message<string, string>
        {
            Key = Guid.NewGuid().ToString(),
            Value = payload,
        }, ct);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
