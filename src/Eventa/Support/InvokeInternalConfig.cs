namespace Eventa;

public sealed class InvokeInternalConfig
{
    public List<EventDefinition<object>> AbortOnEvents { get; } = [];

    public Func<EventEnvelope<object>, Exception?>? MapAbortError { get; set; }
}
