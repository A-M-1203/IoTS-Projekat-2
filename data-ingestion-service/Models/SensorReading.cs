using CsvHelper.Configuration.Attributes;

using System.Text.Json.Serialization;

namespace DataIngestionService.Models;

public class SensorReading
{
    [Name("timestamp")]
    public string Timestamp { get; set; } = "";

    [Name("device_id")]
    public string DeviceId { get; set; } = "";

    [Name("location")]
    public string Location { get; set; } = "";

    [Name("crop_type")]
    public string CropType { get; set; } = "";

    [Name("season")]
    public string Season { get; set; } = "";

    [Name("temperature")]
    public double Temperature { get; set; }

    [Name("humidity")]
    public double Humidity { get; set; }

    [Name("rainfall")]
    public double Rainfall { get; set; }

    [Name("soil_moisture")]
    public double SoilMoisture { get; set; }

    [Name("soil_ph")]
    public double SoilPh { get; set; }

    [Name("light_intensity")]
    public double LightIntensity { get; set; }

    [Name("fertilizer_used")]
    public double FertilizerUsed { get; set; }

    [Name("irrigation_needed")]
    public int IrrigationNeeded { get; set; }

    [Name("crop_health")]
    public string CropHealth { get; set; } = "";

    [Name("yield_estimate")]
    public double YieldEstimate { get; set; }

    [Name("pest_risk")]
    public string PestRisk { get; set; } = "";

    [Name("anomaly_flag")]
    public int AnomalyFlag { get; set; }
}

public class SensorMessage
{
    [JsonPropertyName("message_id")]
    public Guid MessageId { get; set; }

    [JsonPropertyName("published_at")]
    public string PublishedAt { get; set; } = "";

    [JsonPropertyName("timestamp")]
    public string Timestamp { get; set; } = "";

    [JsonPropertyName("device_id")]
    public string DeviceId { get; set; } = "";

    [JsonPropertyName("location")]
    public string Location { get; set; } = "";

    [JsonPropertyName("crop_type")]
    public string CropType { get; set; } = "";

    [JsonPropertyName("season")]
    public string Season { get; set; } = "";

    [JsonPropertyName("temperature")]
    public double Temperature { get; set; }

    [JsonPropertyName("humidity")]
    public double Humidity { get; set; }

    [JsonPropertyName("rainfall")]
    public double Rainfall { get; set; }

    [JsonPropertyName("soil_moisture")]
    public double SoilMoisture { get; set; }

    [JsonPropertyName("soil_ph")]
    public double SoilPh { get; set; }

    [JsonPropertyName("light_intensity")]
    public double LightIntensity { get; set; }

    [JsonPropertyName("fertilizer_used")]
    public double FertilizerUsed { get; set; }

    [JsonPropertyName("irrigation_needed")]
    public int IrrigationNeeded { get; set; }

    [JsonPropertyName("crop_health")]
    public string CropHealth { get; set; } = "";

    [JsonPropertyName("yield_estimate")]
    public double YieldEstimate { get; set; }

    [JsonPropertyName("pest_risk")]
    public string PestRisk { get; set; } = "";

    [JsonPropertyName("anomaly_flag")]
    public int AnomalyFlag { get; set; }
}
