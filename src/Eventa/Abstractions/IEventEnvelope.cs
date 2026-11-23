namespace Eventa;

/// <summary>
/// Represents one emitted event instance after payload type erasure.
/// </summary>
[Obsolete(LegacyTypedApi.Envelope, DiagnosticId = LegacyTypedApi.DiagnosticId)]
public interface IEventEnvelope
{
    /// <summary>
    /// Gets the concrete event identifier that was emitted.
    /// </summary>
    string EventId { get; }

    /// <summary>
    /// Gets the static payload type carried by this envelope.
    /// </summary>
    Type PayloadType { get; }

    /// <summary>
    /// Gets the payload value after type erasure.
    /// </summary>
    object? UntypedBody { get; }
}
