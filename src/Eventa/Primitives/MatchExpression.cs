namespace Eventa;

/// <summary>
/// Describes a reusable payload matcher keyed by an identifier.
/// </summary>
/// <typeparam name="TPayload">The payload type expected by the matcher.</typeparam>
/// <param name="Id">The stable identifier that names this match expression inside a context.</param>
/// <param name="Matcher">The predicate that decides whether an emitted envelope matches.</param>
/// <remarks>
/// Within a single <see cref="EventContext"/>, do not reuse the same <see cref="Id"/> with a
/// different <typeparamref name="TPayload"/>. The default context binds each match expression
/// identifier to one payload type for its lifetime.
/// </remarks>
public sealed record MatchExpression<TPayload>(string Id, Func<EventEnvelope<TPayload>, bool> Matcher)
{
    /// <summary>
    /// Creates a match expression with an optional explicit identifier.
    /// </summary>
    /// <param name="matcher">The predicate that decides whether an emitted envelope matches.</param>
    /// <param name="id">
    /// The optional stable identifier for the expression. When omitted, Eventa generates one.
    /// </param>
    /// <returns>A new <see cref="MatchExpression{TPayload}"/> instance.</returns>
    public static MatchExpression<TPayload> Create(
        Func<EventEnvelope<TPayload>, bool> matcher,
        string? id = null)
    {
        ArgumentNullException.ThrowIfNull(matcher);
        return new MatchExpression<TPayload>(id ?? IdGenerator.New(), matcher);
    }

    /// <summary>
    /// Creates a new match expression that requires both expressions to match the same envelope.
    /// </summary>
    /// <param name="other">The other match expression to combine with this one.</param>
    /// <returns>A new combined expression that applies logical AND semantics.</returns>
    public MatchExpression<TPayload> And(MatchExpression<TPayload> other)
    {
        ArgumentNullException.ThrowIfNull(other);
        return Create(envelope => Matcher(envelope) && other.Matcher(envelope));
    }

    /// <summary>
    /// Creates a new match expression that accepts envelopes matched by either expression.
    /// </summary>
    /// <param name="other">The other match expression to combine with this one.</param>
    /// <returns>A new combined expression that applies logical OR semantics.</returns>
    public MatchExpression<TPayload> Or(MatchExpression<TPayload> other)
    {
        ArgumentNullException.ThrowIfNull(other);
        return Create(envelope => Matcher(envelope) || other.Matcher(envelope));
    }
}
