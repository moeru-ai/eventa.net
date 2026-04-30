using System.Runtime.CompilerServices;

namespace Eventa.Tests;

public class StreamTests
{
    [Fact]
    public async Task InvokeStreamClient_ReturnsServerStreamResults()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<ProfileResponse, UserRequest>("profile");

        using var _ = context.RegisterStreamHandler(definition,
            static (request, _) => ProfileStreamAsync(request));

        var client = context.CreateInvokeStreamClient(definition);
        var results = await CollectAsync(
            client.InvokeAsync(
                new UserRequest("alice", 25),
                CancellationToken.None));

        Assert.Equal<ProfileResponse>(
            [
                new ParametersResponse("alice", 25),
                new ProgressResponse(20),
                new ProgressResponse(40),
                new ProgressResponse(60),
                new ProgressResponse(80),
                new ProgressResponse(100),
                new ResultResponse(true),
            ],
            results);
    }

    [Fact]
    public async Task InvokeStreamClient_SupportsSyncLazyContextFactory()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("lazy-stream");
        var factoryCalls = 0;

        using var _ = context.RegisterStreamHandler(definition,
            static (request, _) => CountAsync(request));

        var client = EventStream.CreateInvokeStreamClient(() =>
        {
            factoryCalls++;
            return context;
        }, definition);

        var results = await CollectAsync(client.InvokeAsync(3, CancellationToken.None));

        Assert.Equal(1, factoryCalls);
        Assert.Equal([1, 2, 3], results);

        static async IAsyncEnumerable<int> CountAsync(int request)
        {
            for (var value = 1; value <= request; value++)
            {
                await Task.Yield();
                yield return value;
            }
        }
    }

    [Fact]
    public async Task ToStreamHandler_SupportsCallbackStyleHandlers()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<ProfileResponse, UserRequest>("callback-stream");

        using var _ = context.RegisterStreamHandler(definition,
            EventStream.ToStreamHandler<ProfileResponse, UserRequest>(static async (request, emit, _) =>
            {
                await emit(new ParametersResponse(request.Name, request.Age));

                for (var progress = 20; progress <= 100; progress += 20)
                {
                    await emit(new ProgressResponse(progress));
                }

                await emit(new ResultResponse(true));
            }));

        var client = context.CreateInvokeStreamClient(definition);
        var results = await CollectAsync(
            client.InvokeAsync(
                new UserRequest("alice", 25),
                CancellationToken.None));

        Assert.Equal<ProfileResponse>(
            [
                new ParametersResponse("alice", 25),
                new ProgressResponse(20),
                new ProgressResponse(40),
                new ProgressResponse(60),
                new ProgressResponse(80),
                new ProgressResponse(100),
                new ResultResponse(true),
            ],
            results);
    }

    [Fact]
    public async Task InvokeStreamClient_IsolatesConcurrentStreams()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<ConcurrentResponse, StreamRequest>("progress");

        using var _ = context.RegisterStreamHandler(definition,
            static (request, _) => ProgressAsync(request));

        var client = context.CreateInvokeStreamClient(definition);
        var aliceTask = CollectAsync(client.InvokeAsync(
            new StreamRequest("alice", 3),
            CancellationToken.None));
        var bobTask = CollectAsync(client.InvokeAsync(
            new StreamRequest("bob", 2),
            CancellationToken.None));
        var cathyTask = CollectAsync(client.InvokeAsync(
            new StreamRequest("cathy", 4),
            CancellationToken.None));

        var results = await Task.WhenAll(aliceTask, bobTask, cathyTask);

        Assert.Equal(
            [
                new ConcurrentProgress("alice", 1),
                new ConcurrentProgress("alice", 2),
                new ConcurrentProgress("alice", 3),
                new ConcurrentResult("alice"),
            ],
            results[0]);
        Assert.Equal(
            [
                new ConcurrentProgress("bob", 1),
                new ConcurrentProgress("bob", 2),
                new ConcurrentResult("bob"),
            ],
            results[1]);
        Assert.Equal(
            [
                new ConcurrentProgress("cathy", 1),
                new ConcurrentProgress("cathy", 2),
                new ConcurrentProgress("cathy", 3),
                new ConcurrentProgress("cathy", 4),
                new ConcurrentResult("cathy"),
            ],
            results[2]);
    }

    [Fact]
    public async Task InvokeStreamClient_SurfacesHandlerErrors()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<string, string>("failing-stream");
        var expected = new InvalidOperationException("stream handler failure");

        using var _ = context.RegisterStreamHandler(definition,
            (string _, CancellationToken _) => ThrowAsync<string>(expected));

        var client = context.CreateInvokeStreamClient(definition);
        var stream = client.InvokeAsync("hello", CancellationToken.None);
        var actual = await Assert.ThrowsAsync<InvalidOperationException>(() => DrainAsync(stream));

        Assert.Same(expected, actual);
    }

    [Fact]
    public async Task InvokeStreamClient_AbortsStreamAndNotifiesHandler()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<string, string>("cancellable-stream");
        var handlerNotified = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        async IAsyncEnumerable<string> Handler(
            string _,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            using var registration = cancellationToken.Register(() => handlerNotified.TrySetResult(true));
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            yield break;
        }

        using var _ = context.RegisterStreamHandler(definition, Handler);

        var client = context.CreateInvokeStreamClient(definition);
        using var cancellationSource = new CancellationTokenSource();
        var stream = client.InvokeAsync("hello", cancellationSource.Token);
        var draining = DrainAsync(stream);

        cancellationSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await draining);
        await handlerNotified.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task InvokeStreamClient_WithPreCanceledToken_EmitsAbortOnlyOnce_WithoutSendingRequest()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<string, string>("pre-canceled-stream");
        var sendEvent = new EventDefinition<SendPayload<string>>(definition.SendEventId);
        var sendAbortEvent = new EventDefinition<AbortPayload>(definition.SendAbortId);
        var sendCount = 0;
        var abortCount = 0;

        using var _ = context.Subscribe(sendEvent, _ => sendCount++);
        using var __ = context.Subscribe(sendAbortEvent, _ => abortCount++);
        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.Cancel();

        var client = context.CreateInvokeStreamClient(definition);
        var stream = client.InvokeAsync("hello", cancellationSource.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => DrainAsync(stream));

        Assert.Equal(0, sendCount);
        Assert.Equal(1, abortCount);
    }

    [Fact]
    public async Task InvokeStreamClient_WithPreCanceledToken_DoesNotEnumerateRequestStreamInput()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("pre-canceled-request-stream");
        var sendEvent = new EventDefinition<SendPayload<int>>(definition.SendEventId);
        var sendAbortEvent = new EventDefinition<AbortPayload>(definition.SendAbortId);
        var requestEnumerationCount = 0;
        var sendCount = 0;
        var abortCount = 0;

        async IAsyncEnumerable<int> Requests()
        {
            requestEnumerationCount++;
            await Task.Yield();
            yield return 1;
        }

        using var _ = context.Subscribe(sendEvent, _ => sendCount++);
        using var __ = context.Subscribe(sendAbortEvent, _ => abortCount++);
        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.Cancel();

        var client = context.CreateInvokeStreamClient(definition);
        var stream = client.InvokeAsync(Requests(), cancellationSource.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => DrainAsync(stream));

        Assert.Equal(0, requestEnumerationCount);
        Assert.Equal(0, sendCount);
        Assert.Equal(1, abortCount);
    }

    [Fact]
    public async Task DisposingAsyncEnumerator_NotifiesTheStreamHandler()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("cancel-stream");
        var handlerNotified = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        async IAsyncEnumerable<int> Handler(
            int request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            using var registration = cancellationToken.Register(() => handlerNotified.TrySetResult(true));

            yield return request;
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        }

        using var _ = context.RegisterStreamHandler(definition, Handler);

        var client = context.CreateInvokeStreamClient(definition);
        var stream = client.InvokeAsync(7, CancellationToken.None);
        await using var enumerator = stream.GetAsyncEnumerator(TestContext.Current.CancellationToken);

        Assert.True(await enumerator.MoveNextAsync());
        Assert.Equal(7, enumerator.Current);

        await enumerator.DisposeAsync();
        await handlerNotified.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task DisposingAsyncEnumerator_BeforeQueuedUnarySend_DoesNotLeaveALateHandlerRunning()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("cancel-unary-before-send");
        var handlerStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCanceled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var disposed = 0;
        var lateHandlerStarts = 0;
        var workerCount = Math.Max(8, Environment.ProcessorCount * 2);
        ThreadPool.GetMinThreads(out var originalMinWorkerThreads, out var originalMinCompletionPortThreads);
        using var releaseWorkers = new ManualResetEventSlim(false);
        using var workersStarted = new CountdownEvent(workerCount);
        var blockers = Enumerable.Range(0, workerCount)
            .Select(_ => Task.Run(() =>
            {
                workersStarted.Signal();
                releaseWorkers.Wait(TestContext.Current.CancellationToken);
            }))
            .ToArray();

        ThreadPool.SetMinThreads(
            Math.Max(originalMinWorkerThreads, workerCount),
            originalMinCompletionPortThreads);

        async IAsyncEnumerable<int> Handler(
            int request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            if (Volatile.Read(ref disposed) != 0)
            {
                Interlocked.Increment(ref lateHandlerStarts);
            }

            handlerStarted.TrySetResult(true);

            using var registration = cancellationToken.Register(() => handlerCanceled.TrySetResult(true));
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            yield return request;
        }

        using var _ = context.RegisterStreamHandler(definition, Handler);

        var client = context.CreateInvokeStreamClient(definition);
        try
        {
            Assert.True(workersStarted.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

            var stream = client.InvokeAsync(7, CancellationToken.None);
            await using var enumerator = stream.GetAsyncEnumerator(TestContext.Current.CancellationToken);

            await enumerator.DisposeAsync();
            Interlocked.Exchange(ref disposed, 1);

            releaseWorkers.Set();
            await Task.WhenAll(blockers).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await handlerCanceled.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

            Assert.Equal(0, Volatile.Read(ref lateHandlerStarts));
        }
        finally
        {
            releaseWorkers.Set();
            ThreadPool.SetMinThreads(originalMinWorkerThreads, originalMinCompletionPortThreads);
        }
    }

    [Fact]
    public async Task DisposingAsyncEnumerator_BeforeQueuedRequestSend_DoesNotEnumerateRequestStreamInput()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("cancel-request-stream-before-send");
        var sendEvent = new EventDefinition<SendPayload<int>>(definition.SendEventId);
        var sendAbortEvent = new EventDefinition<AbortPayload>(definition.SendAbortId);
        var requestEnumerationCount = 0;
        var sendCount = 0;
        var abortCount = 0;
        var workerCount = Math.Max(8, Environment.ProcessorCount * 2);
        ThreadPool.GetMinThreads(out var originalMinWorkerThreads, out var originalMinCompletionPortThreads);
        using var releaseWorkers = new ManualResetEventSlim(false);
        using var workersStarted = new CountdownEvent(workerCount);
        var blockers = Enumerable.Range(0, workerCount)
            .Select(_ => Task.Run(() =>
            {
                workersStarted.Signal();
                releaseWorkers.Wait(TestContext.Current.CancellationToken);
            }))
            .ToArray();

        ThreadPool.SetMinThreads(
            Math.Max(originalMinWorkerThreads, workerCount),
            originalMinCompletionPortThreads);

        async IAsyncEnumerable<int> Requests()
        {
            requestEnumerationCount++;
            await Task.Yield();
            yield return 1;
        }

        async IAsyncEnumerable<int> Handler(
            IAsyncEnumerable<int> request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var value in request.WithCancellation(cancellationToken))
            {
                yield return value;
            }
        }

        using var _ = context.Subscribe(sendEvent, _ => sendCount++);
        using var __ = context.Subscribe(sendAbortEvent, _ => abortCount++);
        using var ___ = context.RegisterStreamHandler(definition, Handler);

        var client = context.CreateInvokeStreamClient(definition);
        try
        {
            Assert.True(workersStarted.Wait(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

            var stream = client.InvokeAsync(Requests(), CancellationToken.None);
            await using var enumerator = stream.GetAsyncEnumerator(TestContext.Current.CancellationToken);

            await enumerator.DisposeAsync();

            releaseWorkers.Set();
            await Task.WhenAll(blockers).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await Task.Delay(200, TestContext.Current.CancellationToken);

            Assert.Equal(0, requestEnumerationCount);
            Assert.Equal(0, sendCount);
            Assert.Equal(1, abortCount);
        }
        finally
        {
            releaseWorkers.Set();
            ThreadPool.SetMinThreads(originalMinWorkerThreads, originalMinCompletionPortThreads);
        }
    }

    [Fact]
    public async Task DisposingAsyncEnumerator_StopsRequestStreamSender_AndPreventsLateRequests()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("dispose-request-stream");
        var firstInvocationCanceled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondInvocationStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowSecondRequest = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var observedRequests = new List<int>();
        var sync = new object();
        var handlerStarts = 0;

        async IAsyncEnumerable<int> Requests([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            yield return 1;

            try
            {
                await allowSecondRequest.Task.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                yield break;
            }

            yield return 2;
        }

        async IAsyncEnumerable<int> Handler(
            IAsyncEnumerable<int> request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var invocation = Interlocked.Increment(ref handlerStarts);
            using var registration = invocation == 1
                ? cancellationToken.Register(() => firstInvocationCanceled.TrySetResult(true))
                : default;

            if (invocation == 2)
            {
                secondInvocationStarted.TrySetResult(true);
            }

            await foreach (var value in request.WithCancellation(cancellationToken))
            {
                lock (sync)
                {
                    observedRequests.Add(value);
                }

                yield return value;
            }
        }

        using var _ = context.RegisterStreamHandler(definition, Handler);

        var client = context.CreateInvokeStreamClient(definition);
        var stream = client.InvokeAsync(
            Requests(TestContext.Current.CancellationToken),
            TestContext.Current.CancellationToken);
        await using var enumerator = stream.GetAsyncEnumerator(TestContext.Current.CancellationToken);

        Assert.True(await enumerator.MoveNextAsync());
        Assert.Equal(1, enumerator.Current);

        await enumerator.DisposeAsync();
        await firstInvocationCanceled.Task.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);

        allowSecondRequest.TrySetResult(true);
        await Task.Delay(200, TestContext.Current.CancellationToken);

        Assert.False(secondInvocationStarted.Task.IsCompleted);
        Assert.Equal(1, Volatile.Read(ref handlerStarts));

        lock (sync)
        {
            Assert.Equal([1], observedRequests);
        }
    }

    [Fact]
    public async Task CompletingResponseStream_StopsRequestStreamSender_AndPreventsLateRequests()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("complete-request-stream");
        var secondInvocationStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowSecondRequest = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var observedRequests = new List<int>();
        var sync = new object();
        var handlerStarts = 0;

        async IAsyncEnumerable<int> Requests([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            yield return 1;

            try
            {
                await allowSecondRequest.Task.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                yield break;
            }

            yield return 2;
        }

        async IAsyncEnumerable<int> Handler(
            IAsyncEnumerable<int> request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var invocation = Interlocked.Increment(ref handlerStarts);
            if (invocation == 2)
            {
                secondInvocationStarted.TrySetResult(true);
            }

            await foreach (var value in request.WithCancellation(cancellationToken))
            {
                lock (sync)
                {
                    observedRequests.Add(value);
                }

                yield return value;
                yield break;
            }
        }

        using var _ = context.RegisterStreamHandler(definition, Handler);

        var client = context.CreateInvokeStreamClient(definition);
        var results = await CollectAsync(
            client.InvokeAsync(
                Requests(TestContext.Current.CancellationToken),
                TestContext.Current.CancellationToken));

        Assert.Equal([1], results);

        allowSecondRequest.TrySetResult(true);
        await Task.Delay(200, TestContext.Current.CancellationToken);

        Assert.False(secondInvocationStarted.Task.IsCompleted);
        Assert.Equal(1, Volatile.Read(ref handlerStarts));

        lock (sync)
        {
            Assert.Equal([1], observedRequests);
        }
    }

    [Fact]
    public async Task InvokeStreamClient_SupportsRequestStreamInput()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("sum-stream");

        using var _ = context.RegisterStreamHandler(definition,
            static (request, cancellationToken) => SumAsync(request, cancellationToken));

        var client = context.CreateInvokeStreamClient(definition);
        var results = await CollectAsync(
            client.InvokeAsync(
                Numbers(1, 2, 3),
                CancellationToken.None));

        Assert.Equal([6], results);
    }

    [Fact]
    public async Task InvokeStreamClient_SupportsEmptyRequestStreamInput()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("sum-stream-empty");

        using var _ = context.RegisterStreamHandler(definition,
            static (request, cancellationToken) => SumAsync(request, cancellationToken));

        var client = context.CreateInvokeStreamClient(definition);
        var results = await CollectAsync(
                client.InvokeAsync(
                    Numbers(),
                    CancellationToken.None))
            .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal([0], results);
    }

    [Fact]
    public async Task RegisterStreamHandler_CompletesEmptyRequestStreamProtocolMessages()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("sum-stream-empty-protocol");
        var invokeId = "invoke-empty";
        var sendStreamEndEvent = new EventDefinition<StreamEndPayload>(definition.SendStreamEndId);
        var receiveEvent = new EventDefinition<ReceivePayload<int>>(definition.ReceiveEventId);
        var receiveErrorEvent = new EventDefinition<ReceiveErrorPayload>(definition.ReceiveErrorId);
        var receiveStreamEndEvent = new EventDefinition<StreamEndPayload>(definition.ReceiveStreamEndId);
        var response = new TaskCompletionSource<ReceivePayload<int>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var streamEnded = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var received = new List<int>();
        var handlerStarts = 0;

        async IAsyncEnumerable<int> Handler(
            IAsyncEnumerable<int> request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref handlerStarts);

            await foreach (var value in request.WithCancellation(cancellationToken))
            {
                received.Add(value);
                yield return value;
            }

            yield return 0;
        }

        using var _ = context.Subscribe(receiveEvent, envelope =>
        {
            if (envelope.Body.InvokeId == invokeId)
            {
                response.TrySetResult(envelope.Body);
            }
        });
        using var __ = context.Subscribe(receiveErrorEvent, envelope =>
        {
            if (envelope.Body.InvokeId == invokeId)
            {
                response.TrySetException(envelope.Body.Error);
            }
        });
        using var ___ = context.Subscribe(receiveStreamEndEvent, envelope =>
        {
            if (envelope.Body.InvokeId == invokeId)
            {
                streamEnded.TrySetResult(true);
            }
        });
        using var ____ = context.RegisterStreamHandler(definition,
            Handler);

        context.Emit(sendStreamEndEvent, new StreamEndPayload(invokeId));

        var result = await response.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await streamEnded.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(1, Volatile.Read(ref handlerStarts));
        Assert.Empty(received);
        Assert.Equal(new ReceivePayload<int>(invokeId, 0), result);
    }

    [Fact]
    public async Task InvokeStreamClient_AbortsRequestStreamAndNotifiesHandler()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("abort-request-stream");
        var handlerNotified = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var received = new List<int>();
        var responses = new List<int>();
        Exception? handlerError = null;
        Exception? readError = null;
        const int writeIntervalMilliseconds = 250;
        const int totalWrites = 10;

        async IAsyncEnumerable<int> Handler(
            IAsyncEnumerable<int> request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            using var registration = cancellationToken.Register(() => handlerNotified.TrySetResult(true));
            await using var enumerator = request.WithCancellation(cancellationToken).GetAsyncEnumerator();

            while (true)
            {
                int value;

                try
                {
                    if (!await enumerator.MoveNextAsync())
                    {
                        yield break;
                    }

                    value = enumerator.Current;
                }
                catch (Exception error)
                {
                    handlerError = error;
                    throw;
                }

                received.Add(value);
                yield return value;
            }
        }

        using var _ = context.RegisterStreamHandler(definition, Handler);
        using var cancellationSource = new CancellationTokenSource();
        var client = context.CreateInvokeStreamClient(definition);
        var stream = client.InvokeAsync(
            PacedNumbers(writeIntervalMilliseconds, totalWrites),
            cancellationSource.Token);
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var readTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var value in stream)
                {
                    responses.Add(value);
                }
            }
            catch (Exception error)
            {
                readError = error;
            }
        }, TestContext.Current.CancellationToken);
        var cancellationTask = Task.Run(async () =>
        {
            await Task.Delay(
                writeIntervalMilliseconds * 4 + 50,
                TestContext.Current.CancellationToken);
            cancellationSource.Cancel();
        }, TestContext.Current.CancellationToken);

        await readTask.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        await cancellationTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await handlerNotified.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        stopwatch.Stop();

        Assert.Equal([1, 2, 3, 4], received);
        Assert.Equal([1, 2, 3, 4], responses);
        Assert.NotNull(handlerError);
        Assert.NotNull(readError);
        Assert.IsAssignableFrom<OperationCanceledException>(handlerError);
        Assert.IsAssignableFrom<OperationCanceledException>(readError);
        Assert.True(stopwatch.ElapsedMilliseconds >= writeIntervalMilliseconds * 4);
        Assert.True(stopwatch.ElapsedMilliseconds < writeIntervalMilliseconds * 5);
    }

    [Fact]
    public async Task InvokeStreamClient_AbortsRequestStreamBeforeFirstItem_AndNotifiesHandler()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("abort-request-stream-before-first-item");
        var receiveErrorEvent = new EventDefinition<ReceiveErrorPayload>(definition.ReceiveErrorId);
        var handlerNotified = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerObservedCancellation = new TaskCompletionSource<OperationCanceledException>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var received = new List<int>();
        var responses = new List<int>();
        var receiveErrorCount = 0;
        Exception? handlerError = null;
        Exception? receiveError = null;
        Exception? readError = null;

        async IAsyncEnumerable<int> Requests([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var requestCanceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var registration = cancellationToken.Register(() => requestCanceled.TrySetResult());
            await requestCanceled.Task.WaitAsync(TestContext.Current.CancellationToken);
            yield break;
        }

        async IAsyncEnumerable<int> Handler(
            IAsyncEnumerable<int> request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            using var registration = cancellationToken.Register(() => handlerNotified.TrySetResult(true));
            await using var enumerator = request.WithCancellation(cancellationToken).GetAsyncEnumerator();

            while (true)
            {
                int value;

                try
                {
                    if (!await enumerator.MoveNextAsync())
                    {
                        yield break;
                    }

                    value = enumerator.Current;
                }
                catch (OperationCanceledException error) when (cancellationToken.IsCancellationRequested)
                {
                    handlerError = error;
                    handlerObservedCancellation.TrySetResult(error);
                    throw;
                }
                catch (Exception error)
                {
                    handlerError = error;
                    handlerObservedCancellation.TrySetException(error);
                    throw;
                }

                received.Add(value);
                yield return value;
            }
        }

        using var errorSubscription = context.Subscribe(receiveErrorEvent, envelope =>
        {
            Interlocked.Increment(ref receiveErrorCount);
            receiveError = envelope.Body.Error;
        });
        using var _ = context.RegisterStreamHandler(definition, Handler);
        using var cancellationSource = new CancellationTokenSource();
        var client = context.CreateInvokeStreamClient(definition);
        var stream = client.InvokeAsync(
            Requests(TestContext.Current.CancellationToken),
            cancellationSource.Token);
        var readTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var value in stream)
                {
                    responses.Add(value);
                }
            }
            catch (Exception error)
            {
                readError = error;
            }
        }, TestContext.Current.CancellationToken);

        cancellationSource.Cancel();

        await readTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await handlerNotified.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        var observedHandlerError = await handlerObservedCancellation.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);

        Assert.Empty(received);
        Assert.Empty(responses);
        Assert.Same(observedHandlerError, handlerError);
        Assert.Null(receiveError);
        Assert.Equal(0, Volatile.Read(ref receiveErrorCount));
        Assert.NotNull(readError);
        Assert.IsAssignableFrom<OperationCanceledException>(readError);
    }

    [Fact]
    public async Task InvokeStreamClient_RequestProducerCanRegisterAfterClientCancellation()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("late-register-after-client-cancellation");
        var producerWaiting = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowLateRegister = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var lateRegisterCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var lateRegisterCallbackInvoked = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Exception? lateRegisterError = null;
        Exception? readError = null;

        async IAsyncEnumerable<int> Requests([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            yield return 1;

            producerWaiting.TrySetResult(true);
            await allowLateRegister.Task.WaitAsync(TestContext.Current.CancellationToken);

            try
            {
                using var registration = cancellationToken.Register(() => lateRegisterCallbackInvoked.TrySetResult(true));
            }
            catch (Exception error)
            {
                lateRegisterError = error;
            }
            finally
            {
                lateRegisterCompleted.TrySetResult();
            }
        }

        using var cancellationSource = new CancellationTokenSource();
        var client = context.CreateInvokeStreamClient(definition);
        var stream = client.InvokeAsync(
            Requests(TestContext.Current.CancellationToken),
            cancellationSource.Token);
        var readTask = Task.Run(async () =>
        {
            try
            {
                await DrainAsync(stream);
            }
            catch (Exception error)
            {
                readError = error;
            }
        }, TestContext.Current.CancellationToken);

        await producerWaiting.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        cancellationSource.Cancel();
        await readTask.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        allowLateRegister.TrySetResult();

        await lateRegisterCompleted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await lateRegisterCallbackInvoked.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Null(lateRegisterError);
        Assert.NotNull(readError);
        Assert.IsAssignableFrom<OperationCanceledException>(readError);
    }

    [Fact]
    public async Task ToStreamHandler_SupportsRequestStreamInput()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, IAsyncEnumerable<int>>("callback-sum-stream");

        using var _ = context.RegisterStreamHandler(definition,
            EventStream.ToStreamHandler<int, IAsyncEnumerable<int>>(async (request, emit, cancellationToken) =>
            {
                var sum = 0;

                await foreach (var value in request.WithCancellation(cancellationToken))
                {
                    sum += value;
                }

                await emit(sum);
            }));

        var client = context.CreateInvokeStreamClient(definition);
        var results = await CollectAsync(
            client.InvokeAsync(
                Numbers(4, 5, 6),
                CancellationToken.None));

        Assert.Equal([15], results);
    }

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source)
    {
        var results = new List<T>();

        await foreach (var value in source)
        {
            results.Add(value);
        }

        return results;
    }

    private static async Task DrainAsync<T>(IAsyncEnumerable<T> source)
    {
        await foreach (var _ in source)
        {
        }
    }

    private static async IAsyncEnumerable<ProfileResponse> ProfileStreamAsync(UserRequest request)
    {
        yield return new ParametersResponse(request.Name, request.Age);

        for (var progress = 20; progress <= 100; progress += 20)
        {
            await Task.Yield();
            yield return new ProgressResponse(progress);
        }

        yield return new ResultResponse(true);
    }

    private static async IAsyncEnumerable<ConcurrentResponse> ProgressAsync(StreamRequest request)
    {
        for (var step = 1; step <= request.Steps; step++)
        {
            await Task.Yield();
            yield return new ConcurrentProgress(request.Name, step);
        }

        yield return new ConcurrentResult(request.Name);
    }

    private static async IAsyncEnumerable<T> ThrowAsync<T>(Exception exception)
    {
        await Task.Yield();
        throw exception;
#pragma warning disable CS0162
        yield return default!;
#pragma warning restore CS0162
    }

    private static async IAsyncEnumerable<int> PacedNumbers(int intervalMilliseconds, int totalWrites)
    {
        for (var value = 1; value <= totalWrites; value++)
        {
            await Task.Delay(intervalMilliseconds);
            yield return value;
        }
    }

    private static async IAsyncEnumerable<int> SumAsync(
        IAsyncEnumerable<int> request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var sum = 0;

        await foreach (var value in request.WithCancellation(cancellationToken))
        {
            sum += value;
        }

        yield return sum;
    }

    private static async IAsyncEnumerable<int> Numbers(params int[] values)
    {
        foreach (var value in values)
        {
            await Task.Yield();
            yield return value;
        }
    }

    private sealed record UserRequest(string Name, int Age);

    private abstract record ProfileResponse;

    private sealed record ParametersResponse(string Name, int Age) : ProfileResponse;

    private sealed record ProgressResponse(int Progress) : ProfileResponse;

    private sealed record ResultResponse(bool Result) : ProfileResponse;

    private sealed record StreamRequest(string Name, int Steps);

    private abstract record ConcurrentResponse;

    private sealed record ConcurrentProgress(string Name, int Step) : ConcurrentResponse;

    private sealed record ConcurrentResult(string Name) : ConcurrentResponse;
}
