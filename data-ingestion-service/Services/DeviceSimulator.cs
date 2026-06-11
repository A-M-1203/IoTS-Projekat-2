using System.Text.Json;
using DataIngestionService.Models;

namespace DataIngestionService.Services;

public class DeviceSimulator : BackgroundService
{
    private readonly IMessagePublisher _publisher;
    private readonly List<SensorReading> _readings;
    private readonly int _deviceCount;
    private readonly int _publishIntervalMs;
    private readonly bool _injectCritical;
    private long _messagesPublished;
    private long _messagesFailed;

    public DeviceSimulator(
        IMessagePublisher publisher,
        List<SensorReading> readings,
        int deviceCount,
        int publishIntervalMs,
        bool injectCritical)
    {
        _publisher = publisher;
        _readings = readings;
        _deviceCount = deviceCount;
        _publishIntervalMs = publishIntervalMs;
        _injectCritical = injectCritical;
    }

    public long MessagesPublished => Interlocked.Read(ref _messagesPublished);
    public long MessagesFailed => Interlocked.Read(ref _messagesFailed);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _publisher.EnsureConnectedAsync(stoppingToken);

        var deviceIds = _readings
            .Select(r => r.DeviceId)
            .Distinct()
            .Take(_deviceCount)
            .ToList();

        if (deviceIds.Count == 0)
        {
            for (var i = 0; i < _deviceCount; i++)
                deviceIds.Add($"SIM-DEV-{i:D4}");
        }

        while (deviceIds.Count < _deviceCount)
        {
            var baseId = deviceIds[deviceIds.Count % Math.Max(deviceIds.Count, 1)];
            deviceIds.Add($"{baseId}-SIM{deviceIds.Count}");
        }

        var deviceReadings = deviceIds.ToDictionary(
            id => id,
            id => _readings.Where(r => r.DeviceId == id).ToList()
        );

        foreach (var id in deviceIds.Where(id => deviceReadings[id].Count == 0))
        {
            deviceReadings[id] = _readings;
        }

        var tasks = deviceIds.Select(id => SimulateDevice(id, deviceReadings[id], stoppingToken));
        await Task.WhenAll(tasks);
    }

    private async Task SimulateDevice(string deviceId, List<SensorReading> readings, CancellationToken ct)
    {
        var index = Random.Shared.Next(readings.Count);
        var criticalCounter = 0;

        while (!ct.IsCancellationRequested)
        {
            var reading = readings[index % readings.Count];
            index++;

            var message = new SensorMessage
            {
                MessageId = Guid.NewGuid(),
                PublishedAt = DateTime.UtcNow.ToString("O"),
                Timestamp = reading.Timestamp,
                DeviceId = deviceId,
                Location = reading.Location,
                CropType = reading.CropType,
                Season = reading.Season,
                Temperature = reading.Temperature,
                Humidity = reading.Humidity,
                Rainfall = reading.Rainfall,
                SoilMoisture = reading.SoilMoisture,
                SoilPh = reading.SoilPh,
                LightIntensity = reading.LightIntensity,
                FertilizerUsed = reading.FertilizerUsed,
                IrrigationNeeded = reading.IrrigationNeeded,
                CropHealth = reading.CropHealth,
                YieldEstimate = reading.YieldEstimate,
                PestRisk = reading.PestRisk,
                AnomalyFlag = reading.AnomalyFlag,
            };

            if (_injectCritical && criticalCounter++ % 50 == 0)
            {
                message.Temperature = 55.0 + Random.Shared.NextDouble() * 10;
                message.PublishedAt = DateTime.UtcNow.ToString("O");
            }

            try
            {
                var json = JsonSerializer.Serialize(message);
                await _publisher.PublishAsync(json, ct);
                Interlocked.Increment(ref _messagesPublished);
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _messagesFailed);
                Console.WriteLine($"Publish failed for {deviceId}: {ex.Message}");
            }

            await Task.Delay(_publishIntervalMs, ct);
        }
    }
}
