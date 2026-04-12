namespace Eventa;

public sealed record MatchExpression<TPayload>(string Id, Func<EventEnvelope<TPayload>, bool> Matcher)
{
    public static MatchExpression<TPayload> Create(
        Func<EventEnvelope<TPayload>, bool> matcher,
        string? id = null)
    {
        throw new NotImplementedException();
    }

    public MatchExpression<TPayload> And(MatchExpression<TPayload> other)
    {
        throw new NotImplementedException();
    }

    public MatchExpression<TPayload> Or(MatchExpression<TPayload> other)
    {
        throw new NotImplementedException();
    }
}
