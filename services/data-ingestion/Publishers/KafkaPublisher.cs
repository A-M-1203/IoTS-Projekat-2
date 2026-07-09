using Confluent.Kafka;

namespace DataIngestion.Publishers;

public class KafkaPublisher : IMessagePublisher
{
    private readonly string _bootstrapServers;
    private readonly string _topic;
    private IProducer<string, string>? _producer;

    public KafkaPublisher(string bootstrapServers, string topic)
    {
        _bootstrapServers = bootstrapServers;
        _topic = topic;
    }

    public Task ConnectAsync(CancellationToken cancellationToken)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            Acks = Acks.All
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
        Console.WriteLine($"Kafka producer configured for {_bootstrapServers}, topic: {_topic}");
        return Task.CompletedTask;
    }

    public async Task PublishAsync(string payload, CancellationToken cancellationToken)
    {
        if (_producer is null)
        {
            throw new InvalidOperationException("Kafka producer is not initialized.");
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await _producer.ProduceAsync(_topic, new Message<string, string>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = payload
                }, cancellationToken);
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Kafka publish failed: {ex.Message}. Retrying in 3s...");
                await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        _producer?.Flush(TimeSpan.FromSeconds(5));
        _producer?.Dispose();
        return ValueTask.CompletedTask;
    }
}
