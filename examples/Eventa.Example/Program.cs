using System.Runtime.CompilerServices;

using Eventa;
using EventaExample.Consumers;
using EventaExample.Contracts;
using EventaExample.Producers;

using Microsoft.Extensions.DependencyInjection;

namespace EventaExample;

internal static class Program
{
    private static async Task Main()
    {
        Console.WriteLine("Eventa C# examples");
        Console.WriteLine("Start with examples 1-5. Later sections cover streaming variants and advanced hooks.");

        RunBasicEvents();
        RunCrossNamespaceEvents();
        RunAotFriendlyDependencyInjection();
        await RunUnaryInvoke();
        await RunServerStreaming();
        await RunStreamingVariants();
        RunContextUtilities();
        await RunOperationalHooks();

        Console.WriteLine();
        Console.WriteLine("Examples completed.");
    }

    private static void RunBasicEvents()
    {
        WriteSection("1. Events: define, subscribe, emit");

        using var context = new EventContext();
        var moved = new EventDefinition<MovePayload>("example:move");

        using var subscription = context.Subscribe(
            moved,
            envelope => WriteLine("received move", $"{envelope.Body.X},{envelope.Body.Y}"));

        context.Emit(moved, new MovePayload(10, 20));
    }

    private static void RunCrossNamespaceEvents()
    {
        WriteSection("2. Events across files and namespaces");

        using var context = new EventContext();
        var projection = new InventoryProjection(context);
        using var subscription = projection.Start();

        var inventory = new InventoryService(context);
        inventory.AdjustStock("SKU-123", 5);
        inventory.AdjustStock("SKU-123", -2);

        WriteLine("shared event id", InventoryEvents.StockAdjusted.Id);
        WriteLines("inventory projection", projection.Changes);
    }

    private static void RunAotFriendlyDependencyInjection()
    {
        WriteSection("3. AOT-friendly dependency injection");

        var services = new ServiceCollection();

        services.AddSingleton<IEventContext>(_ => new EventContext());
        services.AddSingleton<InventoryProjection>();
        services.AddSingleton<InventoryService>();

        using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });
        var projection = provider.GetRequiredService<InventoryProjection>();
        using var subscription = projection.Start();
        var inventory = provider.GetRequiredService<InventoryService>();

        inventory.AdjustStock("SKU-AOT", 7);
        inventory.AdjustStock("SKU-AOT", -1);

        WriteLines("di projection", projection.Changes);
    }

    private static async Task RunUnaryInvoke()
    {
        WriteSection("4. Invoke: request -> response");

        using var context = new EventContext();
        var echo = new InvokeEventDefinition<EchoResponse, EchoRequest>("example:rpc:echo");

        using var handler = context.RegisterInvokeHandler(
            echo,
            static (EchoRequest request, CancellationToken _) =>
                Task.FromResult(new EchoResponse(request.Input.ToUpperInvariant())));

        var client = context.CreateInvokeClient(echo);
        var result = await client.InvokeAsync(new EchoRequest("eventa"));

        WriteLine("echo result", result.Output);
    }

    private static async Task RunServerStreaming()
    {
        WriteSection("5. Stream invoke: request -> many responses");

        using var context = new EventContext();
        var sync = new InvokeEventDefinition<SyncUpdate, SyncRequest>("example:rpc:sync");

        using var handler = context.RegisterStreamHandler(
            sync,
            static (SyncRequest request, CancellationToken cancellationToken) =>
                SyncJob(request, cancellationToken));

        var client = context.CreateInvokeStreamClient(sync);
        var updates = await CollectAsync(
            client.InvokeAsync(new SyncRequest("import", 3)));

        WriteLines("sync updates", updates.Select(Describe));
    }

    private static async Task RunStreamingVariants()
    {
        WriteSection("6. Streaming variants");

        var routeSummary = await RecordRoute();
        var routeChat = await RouteChat();
        var callbackUpdates = await CallbackStyleStream();

        WriteLine("client stream -> unary", $"{routeSummary.Points} points, distance {routeSummary.Distance}");
        WriteLines("bidirectional stream", routeChat.Select(note => note.Message));
        WriteLines("callback stream handler", callbackUpdates.Select(Describe));
    }

    private static async Task<RouteSummary> RecordRoute()
    {
        using var context = new EventContext();
        var recordRoute = new InvokeEventDefinition<RouteSummary, RoutePoint>("example:rpc:record-route");

        using var handler = context.RegisterInvokeHandler(
            recordRoute,
            static async (IAsyncEnumerable<RoutePoint> points, CancellationToken cancellationToken) =>
            {
                var count = 0;
                var distance = 0;

                await foreach (var point in points.WithCancellation(cancellationToken))
                {
                    count++;
                    distance += Math.Abs(point.X) + Math.Abs(point.Y);
                }

                return new RouteSummary(count, distance);
            });

        var client = context.CreateInvokeClient(recordRoute);

        return await client.InvokeAsync(
            RoutePoints(new RoutePoint(1, 2), new RoutePoint(3, 4), new RoutePoint(-2, 5)));
    }

    private static async Task<List<RouteNote>> RouteChat()
    {
        using var context = new EventContext();
        var chat = new InvokeEventDefinition<RouteNote, RouteNote>("example:rpc:route-chat");

        using var handler = context.RegisterStreamHandler(
            chat,
            static (IAsyncEnumerable<RouteNote> request, CancellationToken cancellationToken) =>
                EchoNotes(request, cancellationToken));

        var client = context.CreateInvokeStreamClient(chat);

        return await CollectAsync(
            client.InvokeAsync(RouteNotes(new RouteNote("hello"), new RouteNote("from stream"))));
    }

    private static async Task<List<SyncUpdate>> CallbackStyleStream()
    {
        using var context = new EventContext();
        var sync = new InvokeEventDefinition<SyncUpdate, SyncRequest>("example:rpc:callback-sync");

        using var handler = context.RegisterStreamHandler(
            sync,
            EventStream.ToStreamHandler<SyncUpdate, SyncRequest>(static async (request, emit, cancellationToken) =>
            {
                await emit(new SyncProgress(0));

                for (var step = 1; step <= request.Steps; step++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await emit(new SyncProgress(step * 100 / request.Steps));
                }

                await emit(new SyncCompleted(request.JobId));
            }));

        var client = context.CreateInvokeStreamClient(sync);

        return await CollectAsync(
            client.InvokeAsync(new SyncRequest("callback-import", 2)));
    }

    private static void RunContextUtilities()
    {
        WriteSection("7. Context utilities");

        using var context = new EventContext();
        var stableLog = new EventDefinition<LogEntry>("example:log");
        var generatedLog = new EventDefinition<LogEntry>();
        var calls = new List<string>();

        using var always = context.Subscribe(
            stableLog,
            envelope => calls.Add($"always:{envelope.Body.Message}"));
        using var once = context.SubscribeOnce(
            stableLog,
            envelope => calls.Add($"once:{envelope.Body.Message}"));

        var temporary = context.Subscribe(
            stableLog,
            envelope => calls.Add($"temporary:{envelope.Body.Message}"));

        context.Emit(stableLog, new LogEntry("auth", "info", "boot"));
        temporary.Dispose();
        context.Emit(stableLog, new LogEntry("auth", "error", "token rejected"));

        var isError = MatchExpression<LogEntry>.Create(
            envelope => envelope.Body.Level == "error",
            "example:match:error");
        var fromAuth = MatchExpression<LogEntry>.Create(
            envelope => envelope.Body.Source == "auth",
            "example:match:auth");
        var matched = new List<string>();

        using var matchedSubscription = context.Subscribe(
            isError.And(fromAuth),
            envelope => matched.Add(envelope.Body.Message));

        context.Emit(stableLog, new LogEntry("auth", "error", "token expired"));
        context.Unsubscribe(stableLog);

        WriteLine("stable event id", stableLog.Id);
        WriteLine("generated event id length", generatedLog.Id.Length);
        WriteLines("listener lifecycle", calls);
        WriteLines("matched logs", matched);
    }

    private static async Task RunOperationalHooks()
    {
        WriteSection("8. Advanced: adapters, cancellation, fatal aborts");

        var adapterSummary = ObserveAdapterHooks();
        var unaryCancellation = await CancelUnaryInvoke();
        var streamCancellation = await CancelStreamingInvoke();
        var fatalEvent = await AbortWithFatalEvent();
        var fatalMatch = await AbortWithFatalMatch();

        WriteLine("adapter hooks", adapterSummary);
        WriteLine("unary cancellation", unaryCancellation);
        WriteLine("stream cancellation", streamCancellation);
        WriteLine("fatal event abort", fatalEvent);
        WriteLine("fatal match abort", fatalMatch);
    }

    private static string ObserveAdapterHooks()
    {
        using var adapter = new RecordingAdapter();
        using var context = new EventContext(adapter);
        var log = new EventDefinition<LogEntry>("example:adapter:log");
        var errorLogs = MatchExpression<LogEntry>.Create(
            envelope => envelope.Body.Level == "error",
            "example:adapter:errors");

        using var direct = context.Subscribe(log, _ => { });
        using var matched = context.Subscribe(errorLogs, _ => { });

        context.Emit(
            log,
            new LogEntry("worker", "error", "connection lost"),
            new EmitMetadata("console-example", "corr-1"));

        var sent = adapter.SentCalls.Single();
        var options = (EmitMetadata?)sent.Options;
        var receivedIds = string.Join(" + ", adapter.ReceivedCalls.Select(call => call.EventId));

        return $"received [{receivedIds}], sent {sent.EventId}, options {options?.Source}/{options?.CorrelationId}";
    }

    private static async Task<string> CancelUnaryInvoke()
    {
        using var context = new EventContext();
        var slow = new InvokeEventDefinition<string, string>("example:rpc:cancel-unary");
        var handlerCanceled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var handler = context.RegisterInvokeHandler(
            slow,
            async (string _, CancellationToken cancellationToken) =>
            {
                using var registration = cancellationToken.Register(() => handlerCanceled.TrySetResult(true));
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                return "not reached";
            });

        var client = context.CreateInvokeClient(slow);
        using var cancellationSource = new CancellationTokenSource();
        var pending = client.InvokeAsync("work", cancellationSource.Token);

        cancellationSource.Cancel();

        var errorMessage = await CaptureErrorMessage(() => pending);
        await handlerCanceled.Task.WaitAsync(TimeSpan.FromSeconds(2));

        return errorMessage;
    }

    private static async Task<string> CancelStreamingInvoke()
    {
        using var context = new EventContext();
        var slow = new InvokeEventDefinition<int, int>("example:rpc:cancel-stream");
        var handlerCanceled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var handler = context.RegisterStreamHandler(
            slow,
            (int request, CancellationToken cancellationToken) =>
                SlowCount(request, handlerCanceled, cancellationToken));

        var client = context.CreateInvokeStreamClient(slow);
        using var cancellationSource = new CancellationTokenSource();
        var stream = client.InvokeAsync(3, cancellationSource.Token);

        await using var enumerator = stream.GetAsyncEnumerator();
        var sawFirstItem = await enumerator.MoveNextAsync();
        cancellationSource.Cancel();

        var errorMessage = await CaptureErrorMessage(async () =>
        {
            await enumerator.MoveNextAsync();
        });
        await handlerCanceled.Task.WaitAsync(TimeSpan.FromSeconds(2));

        return $"{(sawFirstItem ? $"first item {enumerator.Current}" : "no items")}, then {errorMessage}";
    }

    private static async Task<string> AbortWithFatalEvent()
    {
        using var context = new EventContext();
        var fatal = new EventDefinition<FatalPayload>("example:fatal:event");
        var pendingWork = new InvokeEventDefinition<string, string>("example:rpc:fatal-event");

        context.RegisterAbortEvent(
            fatal,
            payload => new InvalidOperationException($"fatal event: {payload.Reason}"));

        var client = context.CreateInvokeClient(pendingWork);
        var pending = client.InvokeAsync("request");

        context.Emit(fatal, new FatalPayload("adapter closed"));

        return await CaptureErrorMessage(
            () => pending.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    private static async Task<string> AbortWithFatalMatch()
    {
        using var context = new EventContext();
        var fatalTransport = new EventDefinition<FatalPayload>("example:fatal:transport");
        var anyFatalEvent = MatchExpression<FatalPayload>.Create(
            envelope => envelope.EventId.StartsWith("example:fatal:", StringComparison.Ordinal),
            "example:fatal:any");
        var pendingWork = new InvokeEventDefinition<string, string>("example:rpc:fatal-match");

        context.RegisterAbortEvent(
            anyFatalEvent,
            payload => new InvalidOperationException($"fatal match: {payload.Reason}"));

        var client = context.CreateInvokeClient(pendingWork);
        var pending = client.InvokeAsync("request");

        context.Emit(fatalTransport, new FatalPayload("worker crashed"));

        return await CaptureErrorMessage(
            () => pending.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    private static async IAsyncEnumerable<RoutePoint> RoutePoints(params RoutePoint[] points)
    {
        foreach (var point in points)
        {
            await Task.Yield();
            yield return point;
        }
    }

    private static async IAsyncEnumerable<RouteNote> RouteNotes(params RouteNote[] notes)
    {
        foreach (var note in notes)
        {
            await Task.Yield();
            yield return note;
        }
    }

    private static async IAsyncEnumerable<SyncUpdate> SyncJob(
        SyncRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        for (var step = 1; step <= request.Steps; step++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Yield();
            yield return new SyncProgress(step * 100 / request.Steps);
        }

        yield return new SyncCompleted(request.JobId);
    }

    private static async IAsyncEnumerable<RouteNote> EchoNotes(
        IAsyncEnumerable<RouteNote> request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var note in request.WithCancellation(cancellationToken))
        {
            yield return new RouteNote($"echo: {note.Message}");
        }
    }

    private static async IAsyncEnumerable<int> SlowCount(
        int request,
        TaskCompletionSource<bool> handlerCanceled,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var registration = cancellationToken.Register(() => handlerCanceled.TrySetResult(true));

        yield return request;
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
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

    private static async Task<string> CaptureErrorMessage(Func<Task> action)
    {
        try
        {
            await action();
            return "no error";
        }
        catch (Exception error)
        {
            return $"{error.GetType().Name}: {error.Message}";
        }
    }

    private static string Describe(SyncUpdate update)
    {
        return update switch
        {
            SyncProgress progress => $"progress {progress.Percent}%",
            SyncCompleted completed => $"completed {completed.JobId}",
            _ => update.ToString() ?? string.Empty,
        };
    }

    private static void WriteSection(string title)
    {
        Console.WriteLine();
        Console.WriteLine($"== {title} ==");
    }

    private static void WriteLine<T>(string label, T value)
    {
        Console.WriteLine($"{label}: {value}");
    }

    private static void WriteLines<T>(string label, IEnumerable<T> values)
    {
        Console.WriteLine($"{label}: {string.Join(", ", values)}");
    }

    private sealed record MovePayload(int X, int Y);

    private sealed record LogEntry(string Source, string Level, string Message);

    private sealed record EmitMetadata(string Source, string CorrelationId);

    private sealed record EchoRequest(string Input);

    private sealed record EchoResponse(string Output);

    private sealed record RoutePoint(int X, int Y);

    private sealed record RouteSummary(int Points, int Distance);

    private sealed record SyncRequest(string JobId, int Steps);

    private abstract record SyncUpdate;

    private sealed record SyncProgress(int Percent) : SyncUpdate;

    private sealed record SyncCompleted(string JobId) : SyncUpdate;

    private sealed record RouteNote(string Message);

    private sealed record FatalPayload(string Reason);

    private sealed record AdapterSentCall(string EventId, object? Envelope, object? Options);

    private sealed record AdapterReceivedCall(string EventId, object? Envelope);

    private sealed class RecordingAdapter : IEventaAdapter
    {
        public List<AdapterSentCall> SentCalls { get; } = [];

        public List<AdapterReceivedCall> ReceivedCalls { get; } = [];

        public void OnSent(string eventId, object? envelope, object? options = null)
        {
            SentCalls.Add(new AdapterSentCall(eventId, envelope, options));
        }

        public void OnReceived(string eventId, object? envelope)
        {
            ReceivedCalls.Add(new AdapterReceivedCall(eventId, envelope));
        }

        public void Dispose() { }
    }
}
