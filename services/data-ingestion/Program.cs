using System.Text.Json;
using System.Text.Json.Serialization;
using DataIngestion.Models;
using DataIngestion.Publishers;
using DataIngestion.Services;

var brokerType = Environment.GetEnvironmentVariable("BROKER_TYPE") ?? "mqtt";
var batchSize = int.Parse(Environment.GetEnvironmentVariable("BATCH_SIZE") ?? "20");
var batchIntervalSeconds = int.Parse(Environment.GetEnvironmentVariable("BATCH_INTERVAL_SECONDS") ?? "5");
var csvFilePath = Environment.GetEnvironmentVariable("CSV_FILE_PATH") ?? "/data/BIED_Smart_Agriculture_Dataset.csv";

IMessagePublisher publisher = brokerType.ToLowerInvariant() switch
{
    "kafka" => new KafkaPublisher(
        Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "kafka:9092",
        Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "iot-agriculture-sensors"),
    _ => new MqttPublisher(
        Environment.GetEnvironmentVariable("MQTT_HOST") ?? "mosquitto",
        int.Parse(Environment.GetEnvironmentVariable("MQTT_PORT") ?? "1883"),
        Environment.GetEnvironmentVariable("MQTT_TOPIC") ?? "iot/agriculture/sensors")
};

var jsonOptions = new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
};

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

Console.WriteLine($"Data Ingestion starting with broker: {brokerType}");
Console.WriteLine($"Reading dataset from: {csvFilePath}");

var reader = new CsvSensorReader();
var readings = reader.ReadAll(csvFilePath);
Console.WriteLine($"Loaded {readings.Count} sensor readings from CSV.");

await publisher.ConnectAsync(cts.Token);

Console.WriteLine($"Publishing in batches of {batchSize} every {batchIntervalSeconds}s.");

var index = 0;
while (!cts.Token.IsCancellationRequested && index < readings.Count)
{
    var batch = readings.Skip(index).Take(batchSize).ToList();
    Console.WriteLine($"Reading batch {(index / batchSize) + 1}: {batch.Count} records (index {index + 1}-{index + batch.Count}).");

    foreach (var reading in batch)
    {
        var payload = JsonSerializer.Serialize(reading, jsonOptions);
        await publisher.PublishAsync(payload, cts.Token);
        Console.WriteLine($"Published: device={reading.DeviceId}, temperature={reading.Temperature:F2}");
    }

    index += batch.Count;

    if (index < readings.Count)
    {
        await Task.Delay(TimeSpan.FromSeconds(batchIntervalSeconds), cts.Token);
    }
}

Console.WriteLine($"Finished publishing all {index} sensor readings.");

await publisher.DisposeAsync();
