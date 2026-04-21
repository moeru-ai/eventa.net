namespace Eventa;

/// <summary>
/// Describes a named event and its payload shape.
/// </summary>
/// <remarks>
/// Within a single <see cref="EventContext"/>, do not reuse the same <see cref="Id"/> with a
/// different <typeparamref name="TPayload"/>. The default context treats the identifier as the
/// event's stable protocol identity for its entire lifetime.
/// </remarks>
public sealed record EventDefinition<TPayload>(string Id)
{
    public EventDefinition() : this(IdGenerator.New()) { }
}
