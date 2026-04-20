namespace Eventa;

public sealed record EventDefinition<TPayload>(string Id)
{
    public EventDefinition() : this(IdGenerator.New()) { }
}
