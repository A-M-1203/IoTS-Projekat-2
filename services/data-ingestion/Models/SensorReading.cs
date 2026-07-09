namespace DataIngestion.Models;

public class SensorReading
{
    public string Timestamp { get; set; } = string.Empty;
    public string DeviceId { get; set; } = string.Empty;
    public string Location { get; set; } = string.Empty;
    public string CropType { get; set; } = string.Empty;
    public string Season { get; set; } = string.Empty;
    public double Temperature { get; set; }
    public double Humidity { get; set; }
    public double Rainfall { get; set; }
    public double SoilMoisture { get; set; }
    public double SoilPh { get; set; }
    public double LightIntensity { get; set; }
    public double FertilizerUsed { get; set; }
    public int IrrigationNeeded { get; set; }
    public string CropHealth { get; set; } = string.Empty;
    public double YieldEstimate { get; set; }
    public string PestRisk { get; set; } = string.Empty;
    public int AnomalyFlag { get; set; }
}
