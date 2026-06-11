using System.Text;
using MQTTnet;
using MQTTnet.Client;

namespace DataIngestionService.Services;

public class MqttPublisher : IMessagePublisher, IAsyncDisposable
{
    private readonly IMqttClient _client;
    private readonly MqttClientOptions _options;
    private readonly string _topic;
    private readonly MQTTnet.Protocol.MqttQualityOfServiceLevel _qos;

    public MqttPublisher(string host, int port, string topic, int qos)
    {
        _topic = topic;
        _qos = qos switch
        {
            2 => MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce,
            1 => MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
            _ => MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce,
        };

        var factory = new MqttFactory();
        _client = factory.CreateMqttClient();
        _options = new MqttClientOptionsBuilder()
            .WithTcpServer(host, port)
            .WithClientId($"ingestion-{Guid.NewGuid():N}")
            .WithCleanSession(false)
            .Build();
    }

    public async Task EnsureConnectedAsync(CancellationToken ct)
    {
        if (_client.IsConnected) return;

        for (var i = 0; i < 30; i++)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                await _client.ConnectAsync(_options, ct);
                return;
            }
            catch
            {
                await Task.Delay(2000, ct);
            }
        }
        throw new InvalidOperationException("Could not connect to MQTT broker");
    }

    public async Task PublishAsync(string payload, CancellationToken ct)
    {
        var message = new MqttApplicationMessageBuilder()
            .WithTopic(_topic)
            .WithPayload(Encoding.UTF8.GetBytes(payload))
            .WithQualityOfServiceLevel(_qos)
            .Build();

        await _client.PublishAsync(message, ct);
    }

    public async ValueTask DisposeAsync()
    {
        if (_client.IsConnected)
            await _client.DisconnectAsync();
        _client.Dispose();
    }
}
