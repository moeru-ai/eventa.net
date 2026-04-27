namespace Eventa.Tests;

public class InvocationCancellationTrackerTests
{
    [Fact]
    public void StopTracking_WhenGivenStaleSource_DoesNotRemoveCurrentSourceForTheSameInvoke()
    {
        var tracker = new InvocationCancellationTracker();
        using var stale = tracker.BeginTracking("invoke");
        using var current = tracker.BeginTracking("invoke");

        tracker.StopTracking("invoke", stale);
        tracker.TryCancel("invoke");

        Assert.False(stale.IsCancellationRequested);
        Assert.True(current.IsCancellationRequested);
    }
}
