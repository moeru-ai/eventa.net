namespace Eventa;

/// <summary>
/// Represents one emitted event instance with its concrete identifier and typed payload.
/// </summary>
/// <typeparam name="TPayload">The payload type carried by the envelope.</typeparam>
/// <param name="EventId">The concrete event identifier that was emitted.</param>
/// <param name="Body">The payload value associated with the emitted event.</param>
public sealed record EventEnvelope<TPayload>(string EventId, TPayload Body) : IEventEnvelope
{
    /// <inheritdoc />
    public Type PayloadType => typeof(TPayload);

    /// <inheritdoc />
    object? IEventEnvelope.UntypedBody => Body;
}
