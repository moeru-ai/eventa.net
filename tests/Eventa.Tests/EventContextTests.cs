namespace Eventa.Tests;

public class EventContextTests
{
    [Fact]
    public void On_and_Emit_DispatchEventEnvelope()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        EventEnvelope<TestPayload>? received = null;

        using var _ = context.On(definition, envelope => received = envelope);

        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(
            new EventEnvelope<TestPayload>("test-event", new TestPayload("test")),
            received);
    }

    [Fact]
    public void On_DeduplicatesTheSameHandlerInstance()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var callCount = 0;
        Action<EventEnvelope<TestPayload>> handler = _ => callCount++;

        using var _ = context.On(definition, handler);
        using var __ = context.On(definition, handler);

        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(1, callCount);
    }

    [Fact]
    public void Once_OnlyDispatchesTheFirstMatchingEvent()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var callCount = 0;
        EventEnvelope<TestPayload>? firstEnvelope = null;

        using var _ = context.Once(definition, envelope =>
        {
            callCount++;
            firstEnvelope = envelope;
        });

        context.Emit(definition, new TestPayload("first"));
        context.Emit(definition, new TestPayload("second"));

        Assert.Equal(1, callCount);
        Assert.Equal(
            new EventEnvelope<TestPayload>("test-event", new TestPayload("first")),
            firstEnvelope);
    }

    [Fact]
    public void Off_WithoutHandler_RemovesAllListenersForTheEvent()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var callCount = 0;

        using var _ = context.On(definition, _ => callCount++);

        context.Off(definition);
        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(0, callCount);
    }

    [Fact]
    public void ReturnedSubscription_DisposesTheListener()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var callCount = 0;

        var subscription = context.On(definition, _ => callCount++);

        subscription.Dispose();
        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(0, callCount);
    }

    [Fact]
    public void Off_WithHandler_RemovesOnlyTheRequestedListener()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var strongCalls = 0;
        var weakCalls = 0;
        Action<EventEnvelope<TestPayload>> strongHandler = _ => strongCalls++;
        Action<EventEnvelope<TestPayload>> weakHandler = _ => weakCalls++;

        using var _ = context.On(definition, strongHandler);
        using var __ = context.On(definition, weakHandler);

        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(1, strongCalls);
        Assert.Equal(1, weakCalls);

        context.Off(definition, weakHandler);
        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(2, strongCalls);
        Assert.Equal(1, weakCalls);
    }

    [Fact]
    public void ReturnedSubscription_RemovesOnlyTheRequestedListener()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var strongCalls = 0;
        var weakCalls = 0;
        Action<EventEnvelope<TestPayload>> strongHandler = _ => strongCalls++;
        Action<EventEnvelope<TestPayload>> weakHandler = _ => weakCalls++;

        using var _ = context.On(definition, strongHandler);
        var weakSubscription = context.On(definition, weakHandler);

        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(1, strongCalls);
        Assert.Equal(1, weakCalls);

        weakSubscription.Dispose();
        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(2, strongCalls);
        Assert.Equal(1, weakCalls);
    }

    [Fact]
    public void MatchExpressionSubscriptions_ReceiveOnlyMatchingPayloads()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var expression = new MatchExpression<TestPayload>(
            "starts-with-match",
            envelope => envelope.Body.Value.StartsWith("match", StringComparison.Ordinal));
        var matchedValues = new List<string>();

        using var _ = context.On(expression, envelope => matchedValues.Add(envelope.Body.Value));

        context.Emit(definition, new TestPayload("match-first"));
        context.Emit(definition, new TestPayload("skip"));
        context.Emit(definition, new TestPayload("match-second"));

        Assert.Equal(["match-first", "match-second"], matchedValues);
    }

    private sealed record TestPayload(string Value);
}
