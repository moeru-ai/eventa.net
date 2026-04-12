namespace Eventa;

public sealed record EventEnvelope<TPayload>(string EventId, TPayload Body);
