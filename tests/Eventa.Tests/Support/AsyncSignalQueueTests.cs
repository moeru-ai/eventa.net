namespace Eventa.Tests;

public class AsyncSignalQueueTests
{
    [Fact]
    public async Task DisposeAsync_BeforeTerminal_InvokesOnDisposeOnce()
    {
        var disposeCalls = 0;
        var queue = new AsyncSignalQueue<int>();
        var stream = queue.ReadAll(onDispose: () =>
        {
            Interlocked.Increment(ref disposeCalls);
            return ValueTask.CompletedTask;
        });

        Assert.True(queue.TryWrite(7));

        var enumerator = stream.GetAsyncEnumerator(TestContext.Current.CancellationToken);

        Assert.True(await enumerator.MoveNextAsync());
        Assert.Equal(7, enumerator.Current);

        await enumerator.DisposeAsync();
        await enumerator.DisposeAsync();

        Assert.Equal(1, Volatile.Read(ref disposeCalls));
    }

    [Fact]
    public async Task Complete_AfterValue_EndsEnumeration_AndSkipsOnDispose()
    {
        var disposeCalls = 0;
        var queue = new AsyncSignalQueue<int>();
        var stream = queue.ReadAll(onDispose: () =>
        {
            Interlocked.Increment(ref disposeCalls);
            return ValueTask.CompletedTask;
        });

        Assert.True(queue.TryWrite(7));

        queue.Complete();
        queue.Complete();

        await using var enumerator = stream.GetAsyncEnumerator(TestContext.Current.CancellationToken);

        Assert.True(await enumerator.MoveNextAsync());
        Assert.Equal(7, enumerator.Current);
        Assert.False(await enumerator.MoveNextAsync());
        Assert.False(queue.TryWrite(8));

        await enumerator.DisposeAsync();

        Assert.Equal(0, Volatile.Read(ref disposeCalls));
    }

    [Fact]
    public async Task Fault_AfterValue_ThrowsTheExactError_AndSkipsOnDispose()
    {
        var disposeCalls = 0;
        var expected = new InvalidOperationException("queue failed");
        var queue = new AsyncSignalQueue<int>();
        var stream = queue.ReadAll(onDispose: () =>
        {
            Interlocked.Increment(ref disposeCalls);
            return ValueTask.CompletedTask;
        });

        Assert.True(queue.TryWrite(7));

        queue.Fault(expected);

        await using var enumerator = stream.GetAsyncEnumerator(TestContext.Current.CancellationToken);

        Assert.True(await enumerator.MoveNextAsync());
        Assert.Equal(7, enumerator.Current);

        var actual = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await enumerator.MoveNextAsync().AsTask());

        Assert.Same(expected, actual);

        await enumerator.DisposeAsync();

        Assert.Equal(0, Volatile.Read(ref disposeCalls));
    }

    [Fact]
    public async Task ReadAll_WhenConsumerCancellationIsIgnored_ReadsWithCanceledToken()
    {
        var queue = new AsyncSignalQueue<int>();

        Assert.True(queue.TryWrite(42));

        queue.Complete();

        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.Cancel();

        await using var enumerator = queue.ReadAll(respectConsumerCancellation: false)
            .GetAsyncEnumerator(cancellationSource.Token);

        Assert.True(await enumerator.MoveNextAsync());
        Assert.Equal(42, enumerator.Current);
        Assert.False(await enumerator.MoveNextAsync());
    }
}
