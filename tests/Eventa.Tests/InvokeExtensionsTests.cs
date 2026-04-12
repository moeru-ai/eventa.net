namespace Eventa.Tests;

public class InvokeExtensionsTests
{
    [Fact]
    public async Task RegisterAbortEvent_RejectsPendingInvokesWhenTheAbortEventFires()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<string, string>("pending");
        var fatalEvent = new EventDefinition<object>("fatal-event");

        context.RegisterAbortEvent(fatalEvent);

        var invoke = EventInvoke.DefineInvoke(context, definition);
        var pending = invoke("request", CancellationToken.None);

        var expected = new InvalidOperationException("worker failed");
        context.Emit(fatalEvent, new FatalEventPayload(expected));

        var actual = await Assert.ThrowsAsync<InvalidOperationException>(async () => await pending);
        Assert.Same(expected, actual);
    }

    private sealed record FatalEventPayload(Exception Error);
}
