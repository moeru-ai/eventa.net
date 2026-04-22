namespace Eventa.Tests;

public class DeferredCancellationRegistrationTests
{
    [Fact]
    public void Attach_WhenCalledTwice_DisposeStillCleansUpOriginalRegistration()
    {
        using var cancellationSource = new CancellationTokenSource();
        var registration = new DeferredCancellationRegistration();
        var firstCallbackCount = 0;
        var secondCallbackCount = 0;

        registration.Attach(cancellationSource.Token.Register(() => Interlocked.Increment(ref firstCallbackCount)));

        var error = Assert.Throws<InvalidOperationException>(() =>
            registration.Attach(cancellationSource.Token.Register(() => Interlocked.Increment(ref secondCallbackCount))));

        registration.Dispose();
        cancellationSource.Cancel();

        Assert.Equal("Cancellation registration already attached.", error.Message);
        Assert.Equal(0, Volatile.Read(ref firstCallbackCount));
        Assert.Equal(0, Volatile.Read(ref secondCallbackCount));
    }
}
