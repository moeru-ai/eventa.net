using System.Reflection;
using System.Runtime.CompilerServices;

namespace Eventa.Tests;

public sealed class DeliveryIngressTests
{
    [Fact]
    public void Target_WhenInspected_ExposesOnlyTaskBasedDeliveryIngress()
    {
        MethodInfo method = Assert.Single(typeof(IDeliveryTarget).GetMethods());
        Assert.Equal("AcceptAsync", method.Name);
        Assert.Equal(typeof(Task), method.ReturnType);
        Assert.Equal(typeof(Delivery), Assert.Single(method.GetParameters()).ParameterType);
        Assert.False(typeof(IDeliveryTarget).IsAssignableFrom(typeof(IEventContext)));
        Assert.True(typeof(IEventContext).IsAssignableFrom(typeof(EventContext)));
        Assert.True(typeof(IDeliveryTarget).IsAssignableFrom(typeof(EventContext)));
        Assert.Single(typeof(EventContext).GetMethods(), candidate => candidate.Name == "AcceptAsync");
    }

    [Fact]
    public async Task AcceptAsync_WithCaseSensitiveEventIds_RoutesAndPreservesReferences()
    {
        using EventContext context = EventContext.Create();
        Eventa eventa = new("unknown:event", new object());
        Delivery delivery = Delivery.Create("application/unknown", eventa);
        List<Delivery> received = [];
        using IDisposable first = context.Subscribe(eventa.id, item =>
        {
            received.Add(item);
            return Task.CompletedTask;
        });
        using IDisposable second = context.Subscribe(eventa.id, item =>
        {
            received.Add(item);
            return Task.CompletedTask;
        });
        using IDisposable other = context.Subscribe("Unknown:event", _ => throw new InvalidOperationException());

        await context.AcceptAsync(delivery);

        Assert.Equal(2, received.Count);
        Assert.All(received, item => Assert.Same(delivery, item));
        Assert.Same(eventa, received[0].eventa);
        Assert.Equal(32, delivery.hopsRemaining);
        Assert.Equal(31, delivery.Forward().hopsRemaining);
        await Assert.ThrowsAsync<ArgumentNullException>(() => context.AcceptAsync(null!));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" \t")]
    public async Task EmitAsync_WithEmptyOrWhitespaceEventId_RoutesLocally(string eventId)
    {
        using EventContext context = new();
        Eventa eventa = new(eventId, new object());
        Delivery? received = null;
        using IDisposable registration = context.Subscribe(eventId, item =>
        {
            received = item;
            return Task.CompletedTask;
        });
        await context.EmitAsync("application/example", eventa);
        Assert.NotNull(received);
        Assert.Same(eventa, received.eventa);
        Assert.Equal(32, received.hopsRemaining);
    }

    [Fact]
    public async Task SubscriptionDisposal_WithReentrantListener_RemovesOnlyItsRegistration()
    {
        using EventContext context = new();
        int count = 0;
        IDisposable registration = context.Subscribe("event", _ => { count++; return Task.CompletedTask; });
        registration.Dispose();
        registration.Dispose();
        using IDisposable active = context.Subscribe("event", _ =>
        {
            count++;
            using IDisposable nested = context.Subscribe("other", _ => Task.CompletedTask);
            return context.EmitAsync("application/example", new Eventa("other"));
        });
        registration.Dispose();
        await context.EmitAsync("application/example", new Eventa("event"));
        Assert.Equal(1, count);
        context.Dispose();
        await context.EmitAsync("application/example", new Eventa("event"));
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task AcceptAsync_WithMultipleListeners_WaitsForAllToSucceed()
    {
        using EventContext context = new();
        TaskCompletionSource first = NewSignal();
        TaskCompletionSource second = NewSignal();
        int calls = 0;
        using IDisposable a = context.Subscribe("event", _ => { calls++; return first.Task; });
        using IDisposable b = context.Subscribe("event", _ => { calls++; return second.Task; });
        Task accepted = context.AcceptAsync(NewDelivery());
        Assert.Equal(2, calls);
        Assert.False(accepted.IsCompleted);
        second.SetResult();
        Assert.False(accepted.IsCompleted);
        first.SetResult();
        await accepted.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task AcceptAsync_WithSynchronousFailure_InvokesLaterListenersAndFailsFast()
    {
        using EventContext context = new();
        InvalidOperationException error = new("first failure");
        TaskCompletionSource pending = NewSignal();
        int calls = 0;
        using IDisposable a = context.Subscribe("event", _ => throw error);
        using IDisposable b = context.Subscribe("event", _ => { calls++; return pending.Task; });
        Task accepted = context.AcceptAsync(NewDelivery());
        Assert.Equal(1, calls);
        Assert.Same(error, await Assert.ThrowsAsync<InvalidOperationException>(() =>
            accepted.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken)));
        Assert.False(pending.Task.IsCompleted);
        pending.SetResult();
        Assert.Same(error, await Assert.ThrowsAsync<InvalidOperationException>(() => accepted));
    }

    [Fact]
    public async Task AcceptAsync_WithAsynchronousFailure_PropagatesOriginalErrorWithoutCancellingWork()
    {
        using EventContext context = new();
        TaskCompletionSource failure = NewSignal();
        TaskCompletionSource pending = NewSignal();
        InvalidOperationException error = new("async failure");
        using IDisposable a = context.Subscribe("event", _ => pending.Task);
        using IDisposable b = context.Subscribe("event", _ => failure.Task);
        Task accepted = context.AcceptAsync(NewDelivery());
        Assert.False(accepted.IsCompleted);
        failure.SetException(error);
        Assert.Same(error, await Assert.ThrowsAsync<InvalidOperationException>(() =>
            accepted.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken)));
        Assert.False(pending.Task.IsCompleted);
        pending.SetResult();
    }

    [Fact]
    public async Task AcceptAsync_WithCanceledListenerTask_FaultsAndAllowsOtherWorkToContinue()
    {
        using EventContext context = new();
        CancellationToken cancellationToken = new(canceled: true);
        Task canceled = Task.FromCanceled(cancellationToken);
        TaskCompletionSource pending = NewSignal();
        int pendingCalls = 0;
        using IDisposable first = context.Subscribe("event", _ => canceled);
        using IDisposable second = context.Subscribe("event", _ => { pendingCalls++; return pending.Task; });
        Delivery delivery = NewDelivery();

        Task accepted = context.AcceptAsync(delivery);
        OperationCanceledException error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            accepted.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        Assert.Equal(TaskStatus.Canceled, canceled.Status);
        Assert.Equal(TaskStatus.Faulted, accepted.Status);
        Assert.Equal(cancellationToken, error.CancellationToken);
        Assert.Same(error, accepted.Exception!.InnerException);
        Assert.False(pending.Task.IsCompleted);
        Assert.True(context.AcceptAsync(delivery).IsCompletedSuccessfully);
        Assert.Equal(1, pendingCalls);
        pending.SetResult();
    }

    [Fact]
    public async Task AcceptAsync_WithConcurrentFailures_KeepsOneOriginalException()
    {
        using EventContext context = new();
        TaskCompletionSource first = NewSignal();
        TaskCompletionSource second = NewSignal();
        InvalidOperationException firstError = new("a");
        InvalidOperationException secondError = new("b");
        using IDisposable a = context.Subscribe("event", _ => first.Task);
        using IDisposable b = context.Subscribe("event", _ => second.Task);
        Task accepted = context.AcceptAsync(NewDelivery());
        await Task.WhenAll(
            Task.Run(() => first.SetException(firstError), TestContext.Current.CancellationToken),
            Task.Run(() => second.SetException(secondError), TestContext.Current.CancellationToken));
        InvalidOperationException winner = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            accepted.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        Assert.True(ReferenceEquals(winner, firstError) || ReferenceEquals(winner, secondError));
        Assert.Same(winner, await Assert.ThrowsAsync<InvalidOperationException>(() => accepted));
    }

    [Fact]
    public void AcceptAsync_WithLaterFault_ObservesItBeforeCollection()
    {
        InvalidOperationException laterError = new("later fault");
        int unobserved = 0;
        void OnUnobserved(object? sender, UnobservedTaskExceptionEventArgs args)
        {
            if (args.Exception.InnerExceptions.Contains(laterError))
            {
                Interlocked.Increment(ref unobserved);
            }
        }

        TaskScheduler.UnobservedTaskException += OnUnobserved;
        try
        {
            WeakReference task = CompleteWithLaterFault(laterError);
            for (int attempt = 0; attempt < 10 && task.IsAlive; attempt++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
            }

            Assert.False(task.IsAlive);
            Assert.Equal(0, Volatile.Read(ref unobserved));
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= OnUnobserved;
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference CompleteWithLaterFault(Exception laterError)
    {
        using EventContext context = new();
        TaskCompletionSource later = new(); // Inline observation makes the collection assertion deterministic.
        InvalidOperationException firstError = new("first");
        using IDisposable a = context.Subscribe("event", _ => throw firstError);
        using IDisposable b = context.Subscribe("event", _ => later.Task);
        Task accepted = context.AcceptAsync(NewDelivery());
        Assert.True(accepted.IsFaulted);
        Assert.Same(firstError, accepted.Exception!.InnerException);
        later.SetException(laterError);
        Assert.Same(firstError, accepted.Exception.InnerException);
        return new WeakReference(later.Task);
    }

    [Fact]
    public async Task AcceptAsync_WithConcurrentDuplicateIds_DispatchesOnce()
    {
        using EventContext context = new();
        Delivery delivery = NewDelivery();
        TaskCompletionSource listener = NewSignal();
        TaskCompletionSource admitted = NewSignal();
        int calls = 0;
        using IDisposable registration = context.Subscribe("event", _ =>
        {
            Interlocked.Increment(ref calls);
            admitted.TrySetResult();
            return listener.Task;
        });
        // Task.Run returns the acceptance Task as a value, so pending listener work is not joined.
        Task<Task>[] submissions = [.. Enumerable.Range(0, 128).Select(_ =>
            Task.Run<Task>(() => context.AcceptAsync(delivery), TestContext.Current.CancellationToken))];
        Task[] results = await Task.WhenAll(submissions)
            .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await admitted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(1, Volatile.Read(ref calls));
        _ = Assert.Single(results, result => !result.IsCompleted);
        Assert.Equal(127, results.Count(result => result.IsCompletedSuccessfully));
        listener.SetResult();
        await Task.WhenAll(results).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task AcceptAsync_WithDuplicateIdAndDifferentFields_CompletesIndependently()
    {
        using EventContext context = new();
        Delivery original = NewDelivery();
        TaskCompletionSource listener = NewSignal();
        int calls = 0;
        using IDisposable registration = context.Subscribe("event", _ => { calls++; return listener.Task; });
        using IDisposable other = context.Subscribe("different", _ =>
            throw new InvalidOperationException("must not route"));
        Task first = context.AcceptAsync(original);
        Delivery changed = Delivery.Restore(new DeliveryInput(
            "text/plain", original.deliveryId, 0, new Eventa("different", 12)));
        Task duplicate = context.AcceptAsync(changed);
        Assert.True(duplicate.IsCompletedSuccessfully);
        Assert.False(first.IsCompleted);
        InvalidOperationException error = new("decode failure");
        listener.SetException(error);
        Assert.Same(error, await Assert.ThrowsAsync<InvalidOperationException>(() => first));
        await duplicate;
        await context.AcceptAsync(original with { });
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task AcceptAsync_WithNoListeners_RecordsIdBeforeLaterSubscription()
    {
        using EventContext context = new();
        Delivery delivery = NewDelivery();
        await context.AcceptAsync(delivery);
        int calls = 0;
        using IDisposable registration = context.Subscribe("event", _ => { calls++; return Task.CompletedTask; });
        await context.AcceptAsync(delivery);
        Assert.Equal(0, calls);
        await context.AcceptAsync(NewDelivery());
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task AcceptAsync_WithReentrantDuplicate_SuppressesRedispatch()
    {
        using EventContext context = new();
        int calls = 0;
        using IDisposable registration = context.Subscribe("event", item =>
        {
            calls++;
            return context.AcceptAsync(item);
        });
        await context.AcceptAsync(NewDelivery())
            .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task RecentHistory_WithDefaultTtl_ExpiresAfterFiveMinutesDespiteDuplicates()
    {
        ManualDeliveryTime time = new();
        using EventContext context = new(time, RecentDeliveryHistory.DefaultTtl, RecentDeliveryHistory.DefaultCapacity);
        Delivery delivery = NewDelivery();
        int count = 0;
        using IDisposable registration = context.Subscribe("event", _ => { count++; return Task.CompletedTask; });
        await context.AcceptAsync(delivery);
        time.Advance(TimeSpan.FromMinutes(5) - TimeSpan.FromTicks(1));
        await context.AcceptAsync(delivery);
        Assert.Equal(1, count);
        time.Advance(TimeSpan.FromTicks(1));
        await context.AcceptAsync(delivery);
        Assert.Equal(2, count);
        await context.AcceptAsync(delivery);
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task RecentHistory_WithDefaultCapacity_RetainsTenThousandIdsAndEvictsOnOverflow()
    {
        const int capacity = 10_000;
        using EventContext context = new();
        Delivery[] deliveries = [.. Enumerable.Range(0, capacity).Select(static _ => NewDelivery())];
        int calls = 0;
        using IDisposable registration = context.Subscribe("event", _ => { calls++; return Task.CompletedTask; });
        foreach (Delivery delivery in deliveries)
        {
            await context.AcceptAsync(delivery);
        }

        Assert.Equal(capacity, calls);
        foreach (Delivery delivery in deliveries)
        {
            await context.AcceptAsync(delivery);
        }

        Assert.Equal(capacity, calls);
        await context.AcceptAsync(NewDelivery());
        Assert.Equal(capacity + 1, calls);

        // Any previously retained ID may be evicted. Stop at the first redispatch.
        foreach (Delivery delivery in deliveries)
        {
            await context.AcceptAsync(delivery);
            if (calls > capacity + 1) break;
        }

        Assert.Equal(capacity + 2, calls);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RecentHistory_WithExpiryOrEviction_ReadmitsWhileEarlierWorkIsPending(bool expire)
    {
        ManualDeliveryTime time = new();
        using EventContext context = new(time, TimeSpan.FromSeconds(1), 1);
        TaskCompletionSource pending = NewSignal();
        Delivery delivery = NewDelivery();
        int calls = 0;
        using IDisposable registration = context.Subscribe("event", _ =>
        {
            calls++;
            return calls == 1 ? pending.Task : Task.CompletedTask;
        });
        Task first = context.AcceptAsync(delivery);
        if (expire)
        {
            time.Advance(TimeSpan.FromSeconds(1));
        }
        else
        {
            await context.AcceptAsync(Delivery.Create("application/example", new Eventa("other")));
        }

        await context.AcceptAsync(delivery);
        Assert.Equal(2, calls);
        Assert.False(first.IsCompleted);
        pending.SetResult();
        await first.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task AcceptAsync_WithZeroHopCycle_DispatchesLocallyAndSuppressesDuplicate()
    {
        using EventContext left = new();
        using EventContext right = new();
        int leftCalls = 0;
        int rightCalls = 0;
        Delivery original = Delivery.Restore(new DeliveryInput("application/example", "id", 1, new Eventa("event")));
        using IDisposable a = left.Subscribe("event", _ => { leftCalls++; return Task.CompletedTask; });
        using IDisposable b = right.Subscribe("event", item =>
        {
            rightCalls++;
            Assert.Equal(0, item.hopsRemaining);
            Assert.Throws<InvalidOperationException>(() => item.Forward());
            return left.AcceptAsync(item);
        });
        await left.AcceptAsync(original);
        await right.AcceptAsync(original.Forward());
        Assert.Equal(1, leftCalls);
        Assert.Equal(1, rightCalls);
        Assert.Equal(1, original.hopsRemaining);
    }

    [Fact]
    public async Task AcceptAsync_WithCallerCancelledAfterSubmission_LeavesAcceptedWorkRunning()
    {
        using EventContext context = new();
        using CancellationTokenSource caller = new();
        TaskCompletionSource pending = NewSignal();
        int calls = 0;
        using IDisposable registration = context.Subscribe("event", _ => { calls++; return pending.Task; });
        Delivery delivery = NewDelivery();
        caller.Token.ThrowIfCancellationRequested();
        Task accepted = context.AcceptAsync(delivery);
        caller.Cancel();
        Assert.False(accepted.IsCompleted);
        await context.AcceptAsync(delivery);
        context.Dispose();
        await context.AcceptAsync(delivery);
        Assert.Equal(1, calls);
        pending.SetResult();
        await accepted.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static Delivery NewDelivery() => Delivery.Create("application/example", new Eventa("event"));
}
