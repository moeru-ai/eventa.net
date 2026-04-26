namespace Eventa.Tests;

public class InvokeSessionEngineTests
{
    [Fact]
    public async Task UnaryRun_WhenInlineSendThrows_FaultsTaskWithoutEmittingAbort()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("engine-inline-unary-send-fault");
        var bindings = new InvokeEventBindings<int, int>(definition);
        var sendAbortEvent = new EventDefinition<AbortPayload>(definition.SendAbortId);
        var abortCount = 0;
        var expected = new InvalidOperationException("send failed");

        using var _ = context.Subscribe(sendAbortEvent, _ => abortCount++);

        var engine = new UnaryInvokeSessionEngine<int, int>(
            context,
            bindings,
            SendDispatchMode.InlineAfterCancellationArmed,
            (_, _) => Task.FromException(expected),
            CancellationToken.None);

        var actual = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await engine.Run().WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        Assert.Same(expected, actual);
        Assert.Equal(0, abortCount);
    }

    [Fact]
    public async Task UnaryRun_WhenQueuedSendThrows_FaultsTaskWithoutEmittingAbort()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("engine-queued-unary-send-fault");
        var bindings = new InvokeEventBindings<int, int>(definition);
        var sendAbortEvent = new EventDefinition<AbortPayload>(definition.SendAbortId);
        var abortCount = 0;
        var expected = new InvalidOperationException("queued send failed");

        using var _ = context.Subscribe(sendAbortEvent, _ => abortCount++);

        var engine = new UnaryInvokeSessionEngine<int, int>(
            context,
            bindings,
            SendDispatchMode.QueueOnThreadPool,
            async (_, _) =>
            {
                await Task.Yield();
                throw expected;
            },
            CancellationToken.None);

        var actual = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await engine.Run().WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        Assert.Same(expected, actual);
        Assert.Equal(0, abortCount);
    }

    [Fact]
    public async Task StreamRun_WhenInlineSendThrows_FaultsStreamWithoutEmittingAbort()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("engine-inline-stream-send-fault");
        var bindings = new InvokeEventBindings<int, int>(definition);
        var sendAbortEvent = new EventDefinition<AbortPayload>(definition.SendAbortId);
        var abortCount = 0;
        var expected = new InvalidOperationException("stream send failed");

        using var _ = context.Subscribe(sendAbortEvent, _ => abortCount++);

        var engine = new StreamInvokeSessionEngine<int, int>(
            context,
            bindings,
            SendDispatchMode.InlineAfterCancellationArmed,
            (_, _) => Task.FromException(expected),
            CancellationToken.None);

        var actual = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await DrainAsync(engine.Run()).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        Assert.Same(expected, actual);
        Assert.Equal(0, abortCount);
    }

    [Fact]
    public async Task StreamRun_WhenQueuedSendThrows_FaultsStreamWithoutEmittingAbort()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("engine-queued-stream-send-fault");
        var bindings = new InvokeEventBindings<int, int>(definition);
        var sendAbortEvent = new EventDefinition<AbortPayload>(definition.SendAbortId);
        var abortCount = 0;
        var expected = new InvalidOperationException("queued stream send failed");

        using var _ = context.Subscribe(sendAbortEvent, _ => abortCount++);

        var engine = new StreamInvokeSessionEngine<int, int>(
            context,
            bindings,
            SendDispatchMode.QueueOnThreadPool,
            async (_, _) =>
            {
                await Task.Yield();
                throw expected;
            },
            CancellationToken.None);

        var actual = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await DrainAsync(engine.Run()).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        Assert.Same(expected, actual);
        Assert.Equal(0, abortCount);
    }

    private static async Task DrainAsync<T>(IAsyncEnumerable<T> stream)
    {
        await foreach (var _ in stream.WithCancellation(TestContext.Current.CancellationToken))
        { }
    }
}
