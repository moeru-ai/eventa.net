namespace Eventa.Tests;

public class RequestStreamInvocationTrackerTests
{
    [Fact]
    public async Task GetOrCreate_StartExecutionRunsOutsideTrackerLock()
    {
        var tracker = new RequestStreamInvocationTracker<int>();
        var firstExecutionStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirstExecution = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondExecutionStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var firstGetOrCreate = Task.Run(
            () => tracker.GetOrCreate("first", _ =>
            {
                firstExecutionStarted.TrySetResult(true);
                releaseFirstExecution.Task.GetAwaiter().GetResult();
                return Task.CompletedTask;
            }),
            TestContext.Current.CancellationToken);

        await firstExecutionStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        var secondGetOrCreate = Task.Run(
            () => tracker.GetOrCreate("second", _ =>
            {
                secondExecutionStarted.TrySetResult(true);
                return Task.CompletedTask;
            }),
            TestContext.Current.CancellationToken);

        try
        {
            await secondExecutionStarted.Task.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        }
        finally
        {
            releaseFirstExecution.TrySetResult();
            await firstGetOrCreate.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await secondGetOrCreate.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public void GetOrCreate_WhenStartExecutionThrowsSynchronously_DoesNotKeepBrokenState()
    {
        var tracker = new RequestStreamInvocationTracker<int>();
        var expected = new InvalidOperationException("boom");
        var successfulStarts = 0;

        var error = Assert.Throws<InvalidOperationException>(() =>
            tracker.GetOrCreate("invoke", _ => throw expected));

        var state = tracker.GetOrCreate("invoke", _ =>
        {
            Interlocked.Increment(ref successfulStarts);
            return Task.CompletedTask;
        });

        Assert.Same(expected, error);
        Assert.Equal("invoke", state.InvokeId);
        Assert.Equal(1, Volatile.Read(ref successfulStarts));
        Assert.NotNull(state.Execution);
    }
}
