using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using DataIngestion.Models;

namespace DataIngestion.Services;

public class CsvSensorReader
{
    public List<SensorReading> ReadAll(string filePath)
    {
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HeaderValidated = null,
            MissingFieldFound = null
        };

        using var reader = new StreamReader(filePath);
        using var csv = new CsvReader(reader, config);
        csv.Context.RegisterClassMap<SensorReadingMap>();
        return csv.GetRecords<SensorReading>().ToList();
    }
}
