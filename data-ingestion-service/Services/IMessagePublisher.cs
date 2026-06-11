namespace DataIngestionService.Services;

public interface IMessagePublisher
{
    Task PublishAsync(string payload, CancellationToken ct);
    Task EnsureConnectedAsync(CancellationToken ct);
}
