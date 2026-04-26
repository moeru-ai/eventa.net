namespace Eventa.Tests;

public class InvokeExtensionsTests
{
    [Fact]
    public async Task RegisterAbortEvent_RejectsPendingInvokesWhenTheAbortEventFires()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<string, string>("pending");
        var fatalEvent = new EventDefinition<FatalEventPayload>("fatal-event");

        context.RegisterAbortEvent(fatalEvent, static payload => payload.Error);

        var client = context.CreateInvokeClient(definition);
        var pending = client.InvokeAsync("request", CancellationToken.None);

        var expected = new InvalidOperationException("worker failed");
        context.Emit(fatalEvent, new FatalEventPayload(expected));

        var actual = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pending);
        Assert.Same(expected, actual);
    }

    [Fact]
    public async Task RegisterAbortEvent_WithObjectPayloadException_PreservesTheExactInstance()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<string, string>("pending");
        var fatalEvent = new EventDefinition<object>("fatal-event");

        context.RegisterAbortEvent(fatalEvent);

        var client = context.CreateInvokeClient(definition);
        var pending = client.InvokeAsync("request", CancellationToken.None);

        var expected = new InvalidOperationException("worker failed");
        context.Emit(fatalEvent, expected);

        var actual = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pending);
        Assert.Same(expected, actual);
    }

    [Fact]
    public async Task RegisterAbortEvent_WhenMappedErrorIsNull_UsesDefaultAbortException()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<string, string>("pending");
        var fatalEvent = new EventDefinition<FatalEventPayload>("fatal-event");

        context.RegisterAbortEvent(fatalEvent, static _ => null);

        var client = context.CreateInvokeClient(definition);
        var pending = client.InvokeAsync("request", CancellationToken.None);

        context.Emit(fatalEvent, new FatalEventPayload(new InvalidOperationException("ignored")));

        var actual = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pending);
        Assert.Equal("Pending invoke aborted by fatal event.", actual.Message);
    }

    [Fact]
    public async Task RegisterAbortEvent_WithObjectPayloadThatIsNotException_UsesDefaultAbortException()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<string, string>("pending");
        var fatalEvent = new EventDefinition<object>("fatal-event");

        context.RegisterAbortEvent(fatalEvent);

        var client = context.CreateInvokeClient(definition);
        var pending = client.InvokeAsync("request", CancellationToken.None);

        context.Emit(fatalEvent, "not-an-exception");

        var actual = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pending);
        Assert.Equal("Pending invoke aborted by fatal event.", actual.Message);
    }

    [Fact]
    public async Task RegisterAbortEvent_WithMatchExpression_RejectsPendingInvokesWhenTheMatchFires()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<string, string>("pending");
        var fatalEvent = new EventDefinition<FatalEventPayload>("fatal-event");
        var nonFatalEvent = new EventDefinition<FatalEventPayload>("non-fatal-event");
        var fatalMatch = new MatchExpression<FatalEventPayload>(
            "fatal-match",
            envelope => envelope.EventId.StartsWith("fatal-", StringComparison.Ordinal));

        context.RegisterAbortEvent(fatalMatch, static payload => payload.Error);

        var client = context.CreateInvokeClient(definition);
        var pending = client.InvokeAsync("request", CancellationToken.None);

        context.Emit(nonFatalEvent, new FatalEventPayload(new InvalidOperationException("ignored")));
        Assert.False(pending.IsCompleted);

        var expected = new InvalidOperationException("worker failed");
        context.Emit(fatalEvent, new FatalEventPayload(expected));

        var actual = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pending);
        Assert.Same(expected, actual);
    }

    private sealed record FatalEventPayload(Exception Error);
}
