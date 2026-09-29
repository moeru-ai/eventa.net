namespace Eventa.Tests;

public class MatchExpressionTests
{
    [Fact]
    public void Create_GeneratesAnIdWhenOmitted()
    {
        var expression = MatchExpression<TestPayload>.Create(
            envelope => envelope.Body.Value.StartsWith("match", StringComparison.Ordinal));

        Assert.NotEmpty(expression.Id);
        Assert.True(expression.Matcher(new EventEnvelope<TestPayload>("test-event", new TestPayload("match-value"))));
    }

    [Fact]
    public void Create_PreservesTheSuppliedId()
    {
        var expression = MatchExpression<TestPayload>.Create(
            envelope => envelope.Body.Value.Length > 0,
            "custom-match");

        Assert.Equal("custom-match", expression.Id);
    }

    [Fact]
    public void And_RequiresBothMatchersToSucceed()
    {
        var startsWithA = new MatchExpression<TestPayload>(
            "starts-with-a",
            envelope => envelope.Body.Value.StartsWith('a'));
        var endsWithZ = new MatchExpression<TestPayload>(
            "ends-with-z",
            envelope => envelope.Body.Value.EndsWith('z'));

        var combined = startsWithA.And(endsWithZ);

        Assert.True(combined.Matcher(new EventEnvelope<TestPayload>("test-event", new TestPayload("abz"))));
        Assert.False(combined.Matcher(new EventEnvelope<TestPayload>("test-event", new TestPayload("alpha"))));
        Assert.False(combined.Matcher(new EventEnvelope<TestPayload>("test-event", new TestPayload("buzz"))));
    }

    [Fact]
    public void Or_RequiresEitherMatcherToSucceed()
    {
        var startsWithA = new MatchExpression<TestPayload>(
            "starts-with-a",
            envelope => envelope.Body.Value.StartsWith('a'));
        var endsWithZ = new MatchExpression<TestPayload>(
            "ends-with-z",
            envelope => envelope.Body.Value.EndsWith('z'));

        var combined = startsWithA.Or(endsWithZ);

        Assert.True(combined.Matcher(new EventEnvelope<TestPayload>("test-event", new TestPayload("alpha"))));
        Assert.True(combined.Matcher(new EventEnvelope<TestPayload>("test-event", new TestPayload("buzz"))));
        Assert.False(combined.Matcher(new EventEnvelope<TestPayload>("test-event", new TestPayload("middle"))));
    }

    private sealed record TestPayload(string Value);
}
