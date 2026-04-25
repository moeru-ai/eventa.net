namespace Eventa.Tests;

public class DeferredDisposableTests
{
    [Fact]
    public void Dispose_AfterAttachingDisposable_DisposesOwnedDisposableOnce()
    {
        var deferredDisposable = new DeferredDisposable();
        var disposable = new CountingDisposable();

        deferredDisposable.Attach(disposable);

        deferredDisposable.Dispose();
        deferredDisposable.Dispose();

        Assert.Equal(1, disposable.DisposeCount);
    }

    [Fact]
    public void Attach_WhenCalledTwice_DisposeStillCleansUpOriginalRegistration()
    {
        using var cancellationSource = new CancellationTokenSource();
        var registration = new DeferredDisposable();
        var firstCallbackCount = 0;
        var secondCallbackCount = 0;

        registration.Attach(cancellationSource.Token.Register(() => Interlocked.Increment(ref firstCallbackCount)));

        var error = Assert.Throws<InvalidOperationException>(() =>
            registration.Attach(cancellationSource.Token.Register(() => Interlocked.Increment(ref secondCallbackCount))));

        registration.Dispose();
        cancellationSource.Cancel();

        Assert.Equal("Disposable already attached.", error.Message);
        Assert.Equal(0, Volatile.Read(ref firstCallbackCount));
        Assert.Equal(0, Volatile.Read(ref secondCallbackCount));
    }

    [Fact]
    public void Dispose_BeforeAttach_DisposesRegistrationImmediately()
    {
        using var cancellationSource = new CancellationTokenSource();
        var registration = new DeferredDisposable();
        var callbackCount = 0;

        registration.Dispose();
        registration.Attach(cancellationSource.Token.Register(() => Interlocked.Increment(ref callbackCount)));
        cancellationSource.Cancel();

        Assert.Equal(0, Volatile.Read(ref callbackCount));
    }

    private sealed class CountingDisposable : IDisposable
    {
        public int DisposeCount { get; private set; }

        public void Dispose()
        {
            DisposeCount++;
        }
    }
}
