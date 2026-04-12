namespace Eventa;

public interface IEventaAdapter : IDisposable
{
    void OnSent(string eventId, object? payload, object? options = null);

    void OnReceived(string eventId, object? payload);
}
