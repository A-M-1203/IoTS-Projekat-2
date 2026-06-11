using DataIngestionService.Services;

var builder = WebApplication.CreateBuilder(args);

var brokerType = Environment.GetEnvironmentVariable("BROKER_TYPE") ?? "mqtt";
var csvPath = Environment.GetEnvironmentVariable("CSV_PATH") ?? "/data/BIED_Smart_Agriculture_Dataset.csv";
var deviceCount = int.Parse(Environment.GetEnvironmentVariable("DEVICE_COUNT") ?? "100");
var publishIntervalMs = int.Parse(Environment.GetEnvironmentVariable("PUBLISH_INTERVAL_MS") ?? "100");
var injectCritical = (Environment.GetEnvironmentVariable("INJECT_CRITICAL") ?? "false").Equals("true", StringComparison.OrdinalIgnoreCase);

var loader = new CsvDataLoader();
List<DataIngestionService.Models.SensorReading> readings;

if (File.Exists(csvPath))
{
    readings = loader.Load(csvPath);
    Console.WriteLine($"Loaded {readings.Count} readings from {csvPath}");
}
else
{
    Console.WriteLine($"CSV not found at {csvPath}, using empty dataset");
    readings = [];
}

IMessagePublisher publisher = brokerType.ToLowerInvariant() switch
{
    "kafka" => new KafkaPublisher(
        Environment.GetEnvironmentVariable("KAFKA_BROKERS") ?? "kafka:9092",
        Environment.GetEnvironmentVariable("KAFKA_TOPIC") ?? "iot-agriculture-readings",
        Environment.GetEnvironmentVariable("KAFKA_ACKS") ?? "1"),
    _ => new MqttPublisher(
        Environment.GetEnvironmentVariable("MQTT_HOST") ?? "mosquitto",
        int.Parse(Environment.GetEnvironmentVariable("MQTT_PORT") ?? "1883"),
        Environment.GetEnvironmentVariable("MQTT_TOPIC") ?? "iot/agriculture/readings",
        int.Parse(Environment.GetEnvironmentVariable("MQTT_QOS") ?? "1")),
};

var simulator = new DeviceSimulator(publisher, readings, deviceCount, publishIntervalMs, injectCritical);
builder.Services.AddSingleton(simulator);
builder.Services.AddHostedService(sp => sp.GetRequiredService<DeviceSimulator>());

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new
{
    status = "healthy",
    broker = brokerType,
    deviceCount,
    injectCritical,
}));

app.MapGet("/metrics", () => Results.Ok(new
{
    broker = brokerType,
    deviceCount,
    messagesPublished = simulator.MessagesPublished,
    messagesFailed = simulator.MessagesFailed,
    publishIntervalMs,
    injectCritical,
}));

Console.WriteLine($"Data Ingestion Service starting (broker={brokerType}, devices={deviceCount})");
app.Run();
