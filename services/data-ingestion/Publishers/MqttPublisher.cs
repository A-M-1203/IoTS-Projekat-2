using MQTTnet;
using MQTTnet.Client;

namespace DataIngestion.Publishers;

public class MqttPublisher : IMessagePublisher
{
    private readonly string _host;
    private readonly int _port;
    private readonly string _topic;
    private IMqttClient? _client;

    public MqttPublisher(string host, int port, string topic)
    {
        _host = host;
        _port = port;
        _topic = topic;
    }

    public async Task ConnectAsync(CancellationToken cancellationToken)
    {
        var factory = new MqttFactory();
        _client = factory.CreateMqttClient();

        var options = new MqttClientOptionsBuilder()
            .WithTcpServer(_host, _port)
            .WithClientId($"data-ingestion-{Guid.NewGuid():N}")
            .Build();

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await _client.ConnectAsync(options, cancellationToken);
                Console.WriteLine($"Connected to MQTT broker at {_host}:{_port}");
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"MQTT connection failed: {ex.Message}. Retrying in 3s...");
                await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);
            }
        }
    }

    public async Task PublishAsync(string payload, CancellationToken cancellationToken)
    {
        if (_client is null || !_client.IsConnected)
        {
            throw new InvalidOperationException("MQTT client is not connected.");
        }

        var message = new MqttApplicationMessageBuilder()
            .WithTopic(_topic)
            .WithPayload(payload)
            .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
            .Build();

        await _client.PublishAsync(message, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (_client is not null)
        {
            await _client.DisconnectAsync();
            _client.Dispose();
        }
    }
}
