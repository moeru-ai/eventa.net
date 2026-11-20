namespace Eventa;

/// <summary>
/// Dispatches transport-originated envelopes without re-entering the local send path.
/// </summary>
[Obsolete(LegacyTypedApi.Receive, DiagnosticId = LegacyTypedApi.DiagnosticId)]
public interface IEventInboundDispatcher
{
    /// <summary>
    /// Receives an already-created envelope from a transport and dispatches it locally.
    /// </summary>
    /// <param name="envelope">The transport-originated envelope to dispatch.</param>
    /// <param name="options">Optional adapter metadata associated with the received envelope.</param>
    /// <exception cref="ArgumentNullException"><paramref name="envelope"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// The event identifier is already bound to a different payload type.
    /// </exception>
    void Receive(IEventEnvelope envelope, object? options = null);
}
