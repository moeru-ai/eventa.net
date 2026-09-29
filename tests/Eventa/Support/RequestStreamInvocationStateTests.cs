namespace Eventa.Tests;

public class RequestStreamInvocationStateTests
{
    [Fact]
    public async Task Abort_CancelsTheTokenAndFaultsPendingReaders()
    {
        var testCancellationToken = TestContext.Current.CancellationToken;
        using var state = new RequestStreamInvocationState<int>("invoke");
        await using var enumerator = state.Requests.ReadAll(respectConsumerCancellation: false)
            .GetAsyncEnumerator(testCancellationToken);

        var moveNextTask = enumerator.MoveNextAsync().AsTask();

        state.Abort();

        var error = await Assert.ThrowsAsync<OperationCanceledException>(() => moveNextTask.WaitAsync(testCancellationToken));

        Assert.True(state.CancellationSource.IsCancellationRequested);
        Assert.Equal(state.CancellationSource.Token, error.CancellationToken);
    }
}
