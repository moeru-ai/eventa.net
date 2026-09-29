namespace Eventa.Tests;

public class RunOnceActionTests
{
    [Fact]
    public void Invoke_WhenCalledMultipleTimes_RunsTheCallbackOnlyOnce()
    {
        var callbackCount = 0;
        var action = new RunOnceAction(() => Interlocked.Increment(ref callbackCount));

        action.Invoke();
        action.Invoke();
        action.Invoke();

        Assert.Equal(1, Volatile.Read(ref callbackCount));
    }
}
