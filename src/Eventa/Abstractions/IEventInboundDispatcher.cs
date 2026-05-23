namespace Eventa;

/// <summary>
/// Dispatches transport-originated envelopes without re-entering the local send path.
/// </summary>
public interface IEventInboundDispatcher
{
    /// <summary>
    /// Receives an already-created envelope from a transport and dispatches it locally.
    /// </summary>
    /// <param name="envelope">The transport-originated envelope to dispatch.</param>
    /// <param name="options">Optional adapter metadata associated with the received envelope.</param>
    void Receive(IEventEnvelope envelope, object? options = null);
}
