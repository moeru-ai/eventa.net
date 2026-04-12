namespace Eventa;

public sealed class InvokeInternalConfig
{
    public List<EventDefinition<object>> AbortOnEvents { get; } = new();

    public Func<EventEnvelope<object>, Exception?>? MapAbortError { get; set; }
}
