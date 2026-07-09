using CsvHelper.Configuration;
using DataIngestion.Models;

namespace DataIngestion.Services;

public sealed class SensorReadingMap : ClassMap<SensorReading>
{
    public SensorReadingMap()
    {
        Map(m => m.Timestamp).Name("timestamp");
        Map(m => m.DeviceId).Name("device_id");
        Map(m => m.Location).Name("location");
        Map(m => m.CropType).Name("crop_type");
        Map(m => m.Season).Name("season");
        Map(m => m.Temperature).Name("temperature");
        Map(m => m.Humidity).Name("humidity");
        Map(m => m.Rainfall).Name("rainfall");
        Map(m => m.SoilMoisture).Name("soil_moisture");
        Map(m => m.SoilPh).Name("soil_ph");
        Map(m => m.LightIntensity).Name("light_intensity");
        Map(m => m.FertilizerUsed).Name("fertilizer_used");
        Map(m => m.IrrigationNeeded).Name("irrigation_needed");
        Map(m => m.CropHealth).Name("crop_health");
        Map(m => m.YieldEstimate).Name("yield_estimate");
        Map(m => m.PestRisk).Name("pest_risk");
        Map(m => m.AnomalyFlag).Name("anomaly_flag");
    }
}
