namespace DataIngestion.Publishers;

public interface IMessagePublisher : IAsyncDisposable
{
    Task ConnectAsync(CancellationToken cancellationToken);
    Task PublishAsync(string payload, CancellationToken cancellationToken);
}
