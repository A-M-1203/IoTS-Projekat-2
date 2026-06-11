using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using DataIngestionService.Models;

namespace DataIngestionService.Services;

public class CsvDataLoader
{
    public List<SensorReading> Load(string csvPath)
    {
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            MissingFieldFound = null,
        };

        using var reader = new StreamReader(csvPath);
        using var csv = new CsvReader(reader, config);
        return csv.GetRecords<SensorReading>().ToList();
    }
}
