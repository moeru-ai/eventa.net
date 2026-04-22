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

        var invoke = EventInvoke.DefineInvoke(context, definition);
        var pending = invoke("request", CancellationToken.None);

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

        var invoke = EventInvoke.DefineInvoke(context, definition);
        var pending = invoke("request", CancellationToken.None);

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

        var invoke = EventInvoke.DefineInvoke(context, definition);
        var pending = invoke("request", CancellationToken.None);

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

        var invoke = EventInvoke.DefineInvoke(context, definition);
        var pending = invoke("request", CancellationToken.None);

        context.Emit(fatalEvent, "not-an-exception");

        var actual = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pending);
        Assert.Equal("Pending invoke aborted by fatal event.", actual.Message);
    }

    private sealed record FatalEventPayload(Exception Error);
}
