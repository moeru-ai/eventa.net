namespace Eventa;

/// <summary>
/// Describes a reusable payload matcher keyed by an identifier.
/// </summary>
/// <remarks>
/// Within a single <see cref="EventContext"/>, do not reuse the same <see cref="Id"/> with a
/// different <typeparamref name="TPayload"/>. The default context binds each match expression
/// identifier to one payload type for its lifetime.
/// </remarks>
public sealed record MatchExpression<TPayload>(string Id, Func<EventEnvelope<TPayload>, bool> Matcher)
{
    public static MatchExpression<TPayload> Create(
        Func<EventEnvelope<TPayload>, bool> matcher,
        string? id = null)
    {
        ArgumentNullException.ThrowIfNull(matcher);
        return new MatchExpression<TPayload>(id ?? IdGenerator.New(), matcher);
    }

    public MatchExpression<TPayload> And(MatchExpression<TPayload> other)
    {
        ArgumentNullException.ThrowIfNull(other);
        return Create(envelope => Matcher(envelope) && other.Matcher(envelope));
    }

    public MatchExpression<TPayload> Or(MatchExpression<TPayload> other)
    {
        ArgumentNullException.ThrowIfNull(other);
        return Create(envelope => Matcher(envelope) || other.Matcher(envelope));
    }
}
