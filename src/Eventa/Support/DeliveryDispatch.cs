namespace Eventa;

// Unlike Task.WhenAll, outward failure does not wait for remaining tasks to settle.
internal sealed class DeliveryDispatch(int remaining)
{
    private readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _remaining = remaining;

    /// <summary>Tracks work from callbacks that have already been invoked.</summary>
    /// <param name="pending">The tasks returned by the listeners or representing their synchronous failures.</param>
    /// <returns>A task that fails on an observed listener failure or succeeds after all work succeeds.</returns>
    public static Task CompleteAsync(Task[] pending)
    {
        if (pending.Length == 0) return Task.CompletedTask;

        DeliveryDispatch dispatch = new(pending.Length);
        foreach (Task work in pending)
        {
            // Each observer consumes its task's exception even after outward failure. Observers
            // catch all listener exceptions, so their own tasks cannot produce unobserved faults.
            _ = dispatch.ObserveAsync(work);
        }

        return dispatch._completion.Task;
    }

    private async Task ObserveAsync(Task work)
    {
        try
        {
            await work.ConfigureAwait(false);
        }
        catch (Exception error)
        {
            // TrySetException keeps the first published failure even as other observers finish.
            _completion.TrySetException(error);
        }
        finally
        {
            if (Interlocked.Decrement(ref _remaining) == 0)
            {
                _completion.TrySetResult();
            }
        }
    }
}
