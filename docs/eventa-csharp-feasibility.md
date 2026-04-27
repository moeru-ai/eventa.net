# Eventa -> C# .NET 10 Migration Feasibility Report

## 1. Eventa Architecture Overview

### 1.1 What Eventa Is

Eventa is a **transport-agnostic, type-safe event system** that builds RPC
(request/response) and streaming patterns on top of event primitives. Its core
ideas are:

- **Events are first-class citizens**: define them once and reuse them everywhere
- **The transport layer is pluggable**: the same event definition can work across
  Electron IPC, WebSocket, Web Worker, BroadcastChannel, and other transports
- **RPC is expressed as events**: invoke/stream patterns are composed entirely
  from event primitives without introducing a separate protocol

### 1.2 The Three Core Layers

```
┌─────────────────────────────────────────────────────────┐
│                  Application Layer                      │
│  defineInvoke / defineStreamInvoke / withRemoteMethods  │
├─────────────────────────────────────────────────────────┤
│                   Protocol Layer                        │
│  EventContext (emit / on / once / off)                  │
│  InvokeEventa (7 related events)                        │
│  MatchExpression (glob / regex / custom predicate)      │
├─────────────────────────────────────────────────────────┤
│                   Adapter Layer                         │
│  EventTarget / EventEmitter / WebSocket / Electron /    │
│  BroadcastChannel / WebWorker / WorkerThreads           │
└─────────────────────────────────────────────────────────┘
```

### 1.3 EventContext - The Publish/Subscribe Core

`createContext()` returns an `EventContext` whose internal state includes:

| Data structure | Purpose |
|------|------|
| `Map<EventId, Set<Handler>>` listeners | Persistent listeners |
| `Map<EventId, Set<Handler>>` onceListeners | One-shot listeners removed after firing |
| `Map<MatchExpressionId, MatchExpression>` matchExpressions | Match-expression registry |
| `Map<MatchExpressionId, Set<Handler>>` matchExpressionListeners | Listeners grouped by match expression |

`emit(event, payload)` works like this:

1. Construct `emittingPayload = { ...event, body: payload }`
2. Iterate all listeners for `event.id` and invoke them one by one
3. Iterate all onceListeners for `event.id`, invoke them, and remove them from
   the set afterward
4. Iterate all registered `matchExpressions`, run their `matcher()` against
   `emittingPayload`, and dispatch to that expression's listeners/onceListeners
   when it matches
5. Invoke the adapter hook `onSent(event.id, emittingPayload, options)`

`on()` and `once()` return a `() => void` unsubscribe function.

### 1.4 The Invoke Protocol - A Family of 7 Events

`defineInvokeEventa<Res, Req>()` creates seven related events that share the
same `tag` prefix:

| Event | Direction | Meaning |
|------|------|------|
| `{tag}-send` | Client -> Server | Start a request (`invokeId` + `content`) |
| `{tag}-send-error` | Client -> Server | Error from the client's streaming input |
| `{tag}-send-stream-end` | Client -> Server | End of the client's streaming input |
| `{tag}-send-abort` | Client -> Server | Client canceled the request |
| `{tag}-receive` | Server -> Client | Successful server response |
| `{tag}-receive-error` | Server -> Client | Server handler threw an exception |
| `{tag}-receive-stream-end` | Server -> Client | End of the server's streaming response |

**InvokeId correlation**: every call generates an `invokeId` (16-character
nanoid). Listener event IDs are composed as `{eventId}-{invokeId}` so concurrent
invokes stay isolated from one another.

### 1.5 Invoke Handler Flow

Inside `defineInvokeHandler(ctx, events, handler)` on the server side:

1. Listen to `sendEvent`; after receiving a request, decide whether the input is
   streaming based on `isReqStream`
   - Non-streaming: call `handleInvoke(invokeId, payload)` directly
   - Streaming: create a `ReadableStream` and push each chunk via
     `controller.enqueue()`
2. Listen to `sendEventStreamEnd` and close the request stream controller
3. Listen to `sendEventAbort`, abort the `AbortController`, and error the
   request `ReadableStream` if one exists
4. Execute the handler in `handleInvoke`; send successful results through
   `receiveEvent` and exceptions through `receiveEventError`
5. Return `() => void` so the handler can be unregistered

### 1.6 The Stream Protocol

`defineStreamInvoke` / `defineStreamInvokeHandler` reuse the same seven-event
family as invoke. The differences are:

- **Client side**: returns `ReadableStream<Res>` instead of `Promise<Res>`,
  listens to multiple `receiveEvent`s, and `enqueue()`s values until
  `receiveEventStreamEnd` calls `close()`
- **Server side**: the handler returns `AsyncGenerator<Res>`; each yielded value
  emits one `receiveEvent`, and completion emits `receiveEventStreamEnd`
- `toStreamHandler()` converts callback style (`emit(data)`) into
  `AsyncGenerator` style

### 1.7 The Adapter Pattern

An adapter is a function of the form
`(emit) => { cleanup, hooks: { onSent, onReceived } }`:

- `onSent`: called after every `ctx.emit()`, receiving an envelope shaped like
  `{ ...event, body: payload }`; the adapter serializes it and forwards it over
  the transport
- `onReceived`: called when `ctx.on()`/`ctx.once()` handles a message; it also
  receives an envelope. If the message matched through a match expression, the
  hook's `eventId` may be the match-expression ID, while the original event ID
  is still available on `envelope.EventId`
- Returns a `cleanup` function that disconnects the transport

Existing adapters already cover EventTarget, EventEmitter, BroadcastChannel,
WebSocket (client and H3 server), Electron (main and renderer), WebWorker, and
Worker Threads.

### 1.8 Extension Mechanism

- **Invoke extension**: `withRemoteMethods()` wraps `defineInvoke` /
  `defineInvokeHandler` to serialize and deserialize function stubs (functions
  are replaced with `{ __eventaInvoke: { tag } }`, and the other side
  automatically registers a new invoke handler)
- **Context extension**: `EventContext.extensions`, where adapters can carry
  internal state (for example, `__internal.invoke.abortOnEvents` to reject every
  pending invoke when a worker crashes)
- **Emit extension**: generic `EmitOptions` so adapters can inject extra options
  (for example, `{ raw: { event } }` for original transport events or
  `{ transfer: Transferable[] }` for Structured Clone transfers)

---

## 2. Eventa Test Suite Analysis

### 2.1 Test Framework

- Uses **Vitest** as the test runner
- Uses `vi.fn()` to create mocks and spies
- Uses `expectTypeOf()` for compile-time type assertions
- Uses pure unit tests: every test runs in-process with `createContext()` wired
  directly to itself, without a real transport

### 2.2 Coverage by Test File

#### `context.spec.ts` - EventContext Basics

| Scenario | Description |
|------|------|
| register and emit | Register a handler, emit an event, verify the handler receives `{ ...event, body: payload }` |
| same handler only once | Register the same handler twice; emit should call it only once because a `Set` deduplicates it |
| once listeners | `once()` registration fires only once even if the event is emitted twice |
| off (all) | `off(event)` removes all listeners for that event |
| off (returned) | Calling the function returned by `on()` unsubscribes that one listener |
| off (specific handler) | `off(event, handler)` removes only the specified handler |

#### `invoke.spec.ts` - Unary RPC

| Scenario | Description |
|------|------|
| request-response | Basic request/response and return-value verification |
| lazy context (sync/async) | `defineInvoke(() => ctx)` resolves the context lazily |
| error propagation | Handler throws and the invoke promise rejects with the same error object |
| abort/cancel | `AbortController.abort()` rejects the invoke with `AbortError` and notifies the handler |
| concurrent invokes | Three invokes in parallel that remain isolated |
| same handler once | Registering the same handler twice still results in one active registration |
| undefine handler | `undefineInvokeHandler()` removes one or all handlers |
| batch registration | `defineInvokeHandlers()` + `defineInvokes()` register and invoke in bulk |
| stream input | Request payload is `ReadableStream<number>`; the handler consumes it with `for await` and returns an aggregate |
| abort stream input | Timed request stream (250ms per item), aborted between items 4 and 5; verifies the first four items were received, the handler saw `AbortError`, and elapsed time matches expectations |

#### `stream.spec.ts` - Streaming RPC

| Scenario | Description |
|------|------|
| server-streaming | AsyncGenerator handler; client collects all chunks with `for await` |
| toStreamHandler | Callback-style handler (`emit()`) is equivalent to a generator |
| concurrent streams | Three streaming invokes in parallel with independent results |
| error surfacing | Handler throws and the client sees the same `Error` object during `for await` |
| abort stream | `AbortController.abort()` errors the client stream and notifies the handler |
| cancel stream | `stream.cancel()` notifies the handler |
| abort request stream | Timed request stream + streaming response; abort midway and verify both received items and elapsed time |
| request stream input | `ReadableStream<number>` in, `AsyncGenerator` out; bidirectional streaming |
| toStreamHandler + stream input | Bidirectional streaming with callback-style handler |

#### `invoke-shared.spec.ts` - InvokeEventa Shape

Verifies that `defineInvokeEventa()` produces seven events with the correct
`invokeType` enum values and unique IDs.

#### `invoke-remote-methods.spec.ts` - Function Stub Extension

| Scenario | Description |
|------|------|
| function stubs | Function values in the payload are serialized into stubs and callable on the remote side |
| dispose | Manual cleanup of the generated stub handler |
| maxFunctions | Rejects when the function-count limit is exceeded |
| disallowed tag | Ignore/throw policy for illegal tags |
| prototype pollution (6 cases) | Verifies protection against `__proto__`, `constructor.prototype`, nested paths, array vectors, and similar attacks |
| auto-dispose | Automatically cleans up stub handlers after a timeout |
| strict mode | Throws on malformed stub payloads |

#### `context-extension-invoke-internal.spec.ts` - Adapter Abort Extension

Verifies that events registered via `registerInvokeAbortEventListeners()`
automatically reject all pending invokes.

#### `utils.spec.ts` - Utilities

Positive and negative cases for `isAsyncIterable()` and `isReadableStream()`.

---

## 3. C# .NET 10 Mapping Design

### 3.1 Overall Principle

> **Treat Eventa purely as the protocol layer and a JavaScript reference. The
> C# implementation must follow .NET event-system best practices.**

Key mapping decisions:

| Eventa (TS) | C# .NET 10 | Why |
|------|------|------|
| `defineEventa<P>()` returns a plain object | `record EventDefinition<TPayload>` or `static readonly` instances | Immutable value-like event identities are idiomatic in C# |
| `Eventa<P>.id` is a string | `string Id` property, manually assigned or generated | Keeps compatibility |
| `EventContext` implemented as a closure | `class EventContext` + `IEventContext` | Fits C# OOP conventions |
| `emit(event, payload)` | `void Emit<TPayload>(EventDefinition<TPayload> eventDef, TPayload payload)` | Generic constraints preserve type safety |
| `on()` returns an unsubscribe function | `Subscribe()` returns `IDisposable` subscription token | Matches .NET resource-management conventions (`using`) |
| `Promise<Res>` | `Task<TRes>` / `ValueTask<TRes>` | Native async model in .NET |
| `AbortSignal` / `AbortController` | `CancellationToken` / `CancellationTokenSource` | Native cancellation model in .NET |
| `ReadableStream<T>` | `IAsyncEnumerable<T>` or `Channel<T>` | Native async-stream primitives in .NET |
| `AsyncGenerator` | `async IAsyncEnumerable<T>` | Native C# support since C# 8.0 |
| `vi.fn()` | `NSubstitute` or `Moq` | Standard .NET testing tooling |
| Vitest | xUnit + FluentAssertions | Common .NET test stack |

### 3.2 Event Definitions

```csharp
// Immutable event definition. record provides value-equality semantics.
public record EventDefinition<TPayload>(string Id)
{
    public EventDefinition() : this(IdGenerator.New()) { }
}

// Invoke event definition: the seven related events
public record InvokeEventDefinition<TRes, TReq>(string Tag)
{
    public InvokeEventDefinition() : this(IdGenerator.New()) { }

    public string SendEventId => $"{Tag}-send";
    public string SendErrorId => $"{Tag}-send-error";
    public string SendStreamEndId => $"{Tag}-send-stream-end";
    public string SendAbortId => $"{Tag}-send-abort";
    public string ReceiveEventId => $"{Tag}-receive";
    public string ReceiveErrorId => $"{Tag}-receive-error";
    public string ReceiveStreamEndId => $"{Tag}-receive-stream-end";
}

// Match expression
public record MatchExpression<TPayload>(
    string Id,
    Func<EventEnvelope<TPayload>, bool> Matcher
);

// Definitions are created directly with constructors:
// new EventDefinition<TPayload>()
// new EventDefinition<TPayload>("event-id")
// new InvokeEventDefinition<TRes, TReq>()
// new InvokeEventDefinition<TRes, TReq>("tag")
```

### 3.3 EventContext

```csharp
public interface IEventContext : IDisposable
{
    void Emit<TPayload>(EventDefinition<TPayload> eventDef, TPayload payload);

    IDisposable Subscribe<TPayload>(
        EventDefinition<TPayload> eventDef,
        Action<EventEnvelope<TPayload>> handler);

    IDisposable SubscribeOnce<TPayload>(
        EventDefinition<TPayload> eventDef,
        Action<EventEnvelope<TPayload>> handler);

    void Unsubscribe<TPayload>(
        EventDefinition<TPayload> eventDef,
        Action<EventEnvelope<TPayload>>? handler = null);

    // Match-expression overload
    IDisposable Subscribe<TPayload>(
        MatchExpression<TPayload> match,
        Action<EventEnvelope<TPayload>> handler);
}

// Event envelope, corresponding to TS's { ...event, body: payload }
public record EventEnvelope<TPayload>(string EventId, TPayload Body);
```

**Implementation notes**:

- Use `ConcurrentDictionary<string, ConcurrentBag<Delegate>>` internally for
  thread safety. TS does not need this because it is single-threaded; C# does.
- `Subscribe()` returns `IDisposable`; calling `Dispose()` unsubscribes.
- `SubscribeOnce()` registers a handler that automatically removes itself after the first
  invocation, just like TS.
- Match expressions live in a separate dictionary, and `Emit()` evaluates each
  matcher when dispatching.

### 3.4 Invoke Mapping

```csharp
public static class EventInvoke
{
    /// <summary>
    /// Creates a client-side unary invoke binding.
    /// </summary>
    public static InvokeClient<TRes, TReq> CreateInvokeClient<TRes, TReq>(
        this IEventContext ctx,
        InvokeEventDefinition<TRes, TReq> eventDef);

    /// <summary>
    /// Registers a server-side invoke handler
    /// </summary>
    public static IDisposable RegisterInvokeHandler<TRes, TReq>(
        this IEventContext ctx,
        InvokeEventDefinition<TRes, TReq> eventDef,
        Func<TReq, CancellationToken, Task<TRes>> handler)
    {
        // Listen to sendEvent, extract invokeId, execute the handler,
        // and return the result through receiveEvent
        // Listen to sendAbort and cancel the matching CancellationTokenSource
        // Return IDisposable so the handler registration can be removed
    }
}

public sealed class InvokeClient<TRes, TReq>
{
    public Task<TRes> InvokeAsync(TReq request, CancellationToken ct = default)
    {
        // Generate invokeId, subscribe to receive/error/fatal events,
        // arm cancellation, emit send, and complete the pending task.
        throw new NotImplementedException();
    }
}
```

**C#-specific design points**:

- Use `TaskCompletionSource<TRes>` +
  `TaskCreationOptions.RunContinuationsAsynchronously` to avoid deadlocks
- Use `CancellationToken` instead of `AbortSignal`; the semantics map directly
- Handler signature is `Func<TReq, CancellationToken, Task<TRes>>`, so handler
  code can directly call `ct.ThrowIfCancellationRequested()`
- InvokeId correlation logic is identical to TS (event ID + invokeId)

### 3.5 Context Extensions

```csharp
// Adapter interface
public interface IEventaAdapter : IDisposable
{
    /// <summary>
    /// Called after ctx.emit(); responsible for serializing
    /// EventEnvelope<TPayload> and sending it across the transport.
    /// </summary>
    void OnSent(string eventId, object envelope, object? options = null);

    /// <summary>
    /// Called when ctx.Subscribe()/ctx.SubscribeOnce() matches an incoming message (observation hook).
    /// eventId may be a match-expression id; the original event id remains in envelope.EventId.
    /// </summary>
    void OnReceived(string eventId, object envelope);
}

// Context factory with adapter support
public static IEventContext CreateContext(IEventaAdapter? adapter = null)
{
    return new EventContext(adapter);
}
```

**Context extension points** (equivalent to TS `extensions`):

```csharp
public interface IEventContext
{
    // ... base methods ...

    /// <summary>
    /// Extension property bag where adapters can store internal state
    /// </summary>
    IDictionary<string, object> Extensions { get; }
}

// Internal invoke config (adapter abort extension)
public class InvokeInternalConfig
{
    public List<EventDefinition<object>> AbortOnEvents { get; } = new();
    public Func<EventEnvelope<object>, Exception?>? MapAbortError { get; set; }
}

public static class InvokeExtensions
{
    public static void RegisterAbortEvent(
        this IEventContext ctx,
        EventDefinition<object> fatalEvent)
    {
        // Store it in ctx.Extensions["__internal.invoke"]
    }
}
```

### 3.6 Emit Extensions

In TS, `EmitOptions` is a generic type parameter that adapters can extend with
extra options. A C# equivalent could be:

```csharp
// Basic emit
void Emit<TPayload>(EventDefinition<TPayload> eventDef, TPayload payload);

// Emit with adapter-specific options
void Emit<TPayload, TOptions>(
    EventDefinition<TPayload> eventDef,
    TPayload payload,
    TOptions options) where TOptions : class;

// Example usage (SignalR adapter)
ctx.Emit(moveEvent, new MoveData(100, 200), new SignalROptions
{
    GroupName = "room-1",
    ExcludeConnectionId = connectionId
});
```

### 3.7 Stream Mapping

```csharp
public static class EventStream
{
    /// <summary>
    /// Creates a client-side streaming invoke binding.
    /// </summary>
    public static InvokeStreamClient<TRes, TReq> CreateInvokeStreamClient<TRes, TReq>(
        this IEventContext ctx,
        InvokeEventDefinition<TRes, TReq> eventDef);

    /// <summary>
    /// Registers a streaming handler (using async yield)
    /// </summary>
    public static IDisposable RegisterStreamHandler<TRes, TReq>(
        this IEventContext ctx,
        InvokeEventDefinition<TRes, TReq> eventDef,
        Func<TReq, CancellationToken, IAsyncEnumerable<TRes>> handler)
    {
        // Each yielded value -> emit receiveEvent
        // End of enumeration -> emit receiveEventStreamEnd
        // Exception -> emit receiveEventError
    }
}

public sealed class InvokeStreamClient<TRes, TReq>
{
    public IAsyncEnumerable<TRes> InvokeAsync(TReq request, CancellationToken ct = default)
    {
        // Return IAsyncEnumerable<TRes>; internally bridge through AsyncSignalQueue<TRes>
        // receiveEvent -> queue.TryWrite()
        // receiveEventStreamEnd -> queue.Complete()
        // receiveEventError -> queue.Fault(exception)
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<TRes> InvokeAsync(IAsyncEnumerable<TReq> request, CancellationToken ct = default)
    {
        // Send each request item and complete send-stream-end after input is exhausted.
        throw new NotImplementedException();
    }
}
```

**C#-specific advantages**:

- `IAsyncEnumerable<T>` is a first-class feature in C# 8.0+ and maps directly
  to TS `AsyncGenerator`
- `Channel<T>` (`System.Threading.Channels`) works well as a high-performance
  internal buffer
- `[EnumeratorCancellation]` integrates `CancellationToken` naturally with
  `await foreach`
- Bidirectional streaming can use `IAsyncEnumerable<TReq>` input and
  `IAsyncEnumerable<TRes>` output

```csharp
// Bidirectional stream handler signature
Func<IAsyncEnumerable<TReq>, CancellationToken, IAsyncEnumerable<TRes>> bidiHandler;

// Usage
var client = ctx.CreateInvokeStreamClient(events);
await foreach (var response in client.InvokeAsync(inputStream, ct))
{
    Console.WriteLine(response);
}
```

### 3.8 `toStreamHandler` Equivalent

In TS, `toStreamHandler` converts callback-style `emit(data)` code into an
`AsyncGenerator`. A direct C# equivalent is:

```csharp
public static Func<TReq, CancellationToken, IAsyncEnumerable<TRes>>
    ToStreamHandler<TReq, TRes>(
        Func<TReq, Action<TRes>, CancellationToken, Task> callback)
{
    return (req, ct) => StreamFromCallback(req, callback, ct);
}

private static async IAsyncEnumerable<TRes> StreamFromCallback<TReq, TRes>(
    TReq req,
    Func<TReq, Action<TRes>, CancellationToken, Task> callback,
    [EnumeratorCancellation] CancellationToken ct)
{
    var channel = Channel.CreateUnbounded<TRes>();

    _ = Task.Run(async () =>
    {
        try
        {
            await callback(req, item => channel.Writer.TryWrite(item), ct);
            channel.Writer.Complete();
        }
        catch (Exception ex)
        {
            channel.Writer.Complete(ex);
        }
    }, ct);

    await foreach (var item in channel.Reader.ReadAllAsync(ct))
    {
        yield return item;
    }
}
```

### 3.9 Current Repository Implementation Status (2026-04)

The C# snippets above explain the mapping direction. The prototype in the
current repository has already converged on more concrete implementation
boundaries and differs from the early sketch in several clear ways:

- `InvokeClient` and `InvokeStreamClient` remain distinct public client types,
  but their per-call lifecycle now flows through shared internal
  `UnaryInvokeSessionEngine<TResponse, TRequest>` and
  `StreamInvokeSessionEngine<TResponse, TRequest>` implementations.
- `EventContext` delegates listener storage, payload-type binding, one-shot
  removal, and match-expression snapshotting to `EventListenerStore`, so the
  public context class stays focused on Eventa dispatch semantics and adapter
  notification ordering.
- Streaming currently uses `AsyncSignalQueue<T>` instead of the earlier
  `Channel<T>` sketch from this report, because the current implementation needs
  explicit `Complete` / `Fault` semantics and `onDispose` callbacks at the same
  time.
- Cancellation checks are intentionally preserved at multiple async boundaries
  rather than being collapsed into "just once". This prevents late request /
  late response item emits and also prevents a canceled operation from still
  sending `ReceiveStreamEnd` or still starting the handler.

The shared internal support types extracted in this round are:

| Type | Current responsibility |
|------|----------|
| `InvokeEventBindings<TResponse, TRequest>` | Materializes all send / receive event definitions from `InvokeEventDefinition` in one place |
| `EventListenerStore` | Owns direct and match-expression listener buckets, once-listener removal, dispatch snapshots, and per-context payload-type bindings |
| `Disposables` helpers (`RunOnceAction`, `ActionDisposable`, `DeferredDisposable`, `ClientCancellation`) | Centralize at-most-once cleanup, placeholder disposables, and client-side cancellation registration race handling |
| `HandlerRegistration` | Combines protocol subscriptions and inflight cleanup into a single `IDisposable` |
| `InvocationCancellationTracker` | Tracks the unary handler `invokeId -> CancellationTokenSource` map |
| `RequestStreamInvocationState<TRequest>` | Holds the request queue, cancellation source, and execution task for request-stream handlers |
| `RequestStreamInvocationTracker<TRequest>` | Lazily creates and publishes request-stream state, starts the handler outside the lock, and owns abort / dispose for inflight state |
| `InvokeHandlerRegistrationFactory` | Keeps handler-side protocol subscriptions out of the public API facade classes, wires all four request/response shape combinations, and uses internal handler-session objects to avoid repeating context/events/invoke-id parameters |
| `EventContextFeatures` | Provides typed access to `IEventContext.Extensions` for invoke-internal state without scattering string-key/object-cast logic |

The current implementation still preserves several constraints that are already
anchored by tests and should not be casually erased in future refactors:

- client-side session startup must arm response/error subscriptions and
  cancellation before sending because a fatal-event subscription may complete
  the invoke before the request emit
- The request-stream sender in `InvokeStreamClient` must stop enumerating input and
  must not emit late request items after early client cancellation or early
  enumerator disposal
- The request-stream handler contract still supports "pre-first-item abort" in
  the C# prototype, even though the current TypeScript implementation is more
  conservative when handling unknown invoke IDs on `send-stream-end`
- one-shot event and match-expression listeners are removed before callbacks run
  so re-entrant emits do not invoke the same one-shot listener twice

---

## 4. Adapter Abstraction Layer

### 4.1 Existing Eventa Adapters and Their C# Equivalents

| TS adapter | Equivalent C# /.NET transport | Implementation difficulty |
|------|------|------|
| EventTarget | In-memory direct wiring (`EventContext` itself) | Low |
| EventEmitter | In-memory direct wiring / custom `IObservable<T>` | Low |
| BroadcastChannel | `System.Threading.Channels` / in-process pipes | Low |
| WebSocket (client) | `System.Net.WebSockets.ClientWebSocket` | Medium |
| WebSocket (H3 server) | ASP.NET Core WebSocket middleware / SignalR | Medium |
| Electron Main/Renderer | Not applicable (Electron is JS-specific) | N/A |
| Web Worker | Not applicable | N/A |
| Worker Threads | `System.Threading` / `Channel` across threads | Medium |

### 4.2 Recommended First Adapters

1. **InMemory** (`EventContext` loopback, mainly for tests) - Priority P0
2. **Channel** (cross-thread / in-process pipes) - Priority P0
3. **WebSocket** (`ClientWebSocket` + ASP.NET Core) - Priority P1
4. **SignalR** - Priority P2. SignalR already has built-in streaming hub support,
   but Eventa would still unify the API surface
5. **gRPC** (`Grpc.Net.Client` / `Grpc.AspNetCore`) - Priority P2

### 4.3 Adapter Hook Mapping

```csharp
public interface IEventaAdapter : IDisposable
{
    void OnSent(string eventId, object envelope, object? options = null);
    void OnReceived(string eventId, object envelope);
}

// Example WebSocket adapter
public class WebSocketAdapter : IEventaAdapter
{
    private readonly WebSocket _ws;

    public void OnSent(string eventId, object envelope, object? options = null)
    {
        var json = JsonSerializer.Serialize(new { eventId, envelope });
        var bytes = Encoding.UTF8.GetBytes(json);
        _ws.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
    }

    public void OnReceived(string eventId, object envelope)
    {
        // Observation hook for logging/metrics. If eventId is a match-expression id,
        // the original event id is still available through envelope.EventId.
    }

    // Start the receive loop in the constructor and emit into the context from there
}
```

---

## 5. Test Strategy Mapping

### 5.1 Framework Mapping

| TS (Vitest) | C# (.NET 10) |
|------|------|
| `describe` / `it` | xUnit `[Fact]` / `[Theory]` + nested classes |
| `vi.fn()` | NSubstitute `Substitute.For<T>()` or Moq `Mock<T>()` |
| `expect(x).toBe(y)` | FluentAssertions `x.Should().Be(y)` |
| `expect(promise).rejects.toThrowError()` | `await act.Should().ThrowAsync<Exception>()` |
| `expectTypeOf<T>()` | Compile-time tests (already native in a strongly typed C# language) |
| `await sleep(ms)` | `await Task.Delay(ms)` |

### 5.2 Outline of Equivalent C# Test Shapes

#### EventContext Tests

```csharp
public class EventContextTests
{
    [Fact]
    public void Should_RegisterAndEmit()
    {
        var ctx = EventContext.Create();
        var testEvent = new EventDefinition<TestData>();
        var received = new List<EventEnvelope<TestData>>();

        using var sub = ctx.Subscribe(testEvent, e => received.Add(e));
        ctx.Emit(testEvent, new TestData("test"));

        received.Should().ContainSingle()
            .Which.Body.Value.Should().Be("test");
    }

    [Fact]
    public void Should_HandleOnce()
    {
        var ctx = EventContext.Create();
        var testEvent = new EventDefinition<string>();
        var count = 0;

        using var sub = ctx.SubscribeOnce(testEvent, _ => count++);
        ctx.Emit(testEvent, "a");
        ctx.Emit(testEvent, "b");

        count.Should().Be(1);
    }

    [Fact]
    public void Should_RemoveListenerViaDispose()
    {
        var ctx = EventContext.Create();
        var testEvent = new EventDefinition<string>();
        var count = 0;

        var sub = ctx.Subscribe(testEvent, _ => count++);
        sub.Dispose();
        ctx.Emit(testEvent, "test");

        count.Should().Be(0);
    }
}
```

#### Invoke Tests

```csharp
public class InvokeTests
{
    [Fact]
    public async Task Should_HandleRequestResponse()
    {
        var ctx = EventContext.Create();
        var events = new InvokeEventDefinition<UserResponse, UserRequest>();

        using var handler = ctx.RegisterInvokeHandler(events, (req, ct) => Task.FromResult(new UserResponse($"user-{req.Name}")));
        var client = ctx.CreateInvokeClient(events);

        var result = await client.InvokeAsync(new UserRequest("alice"), CancellationToken.None);

        result.Id.Should().Be("user-alice");
    }

    [Fact]
    public async Task Should_PropagateHandlerErrors()
    {
        var ctx = EventContext.Create();
        var events = new InvokeEventDefinition<string, string>();

        using var handler = ctx.RegisterInvokeHandler(events, (_, _) => throw new InvalidOperationException("handler failed"));
        var client = ctx.CreateInvokeClient(events);

        var act = () => client.InvokeAsync("test", CancellationToken.None);
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("handler failed");
    }

    [Fact]
    public async Task Should_CancelViaToken()
    {
        var ctx = EventContext.Create();
        var events = new InvokeEventDefinition<string, int>();
        var cts = new CancellationTokenSource();

        using var handler = ctx.RegisterInvokeHandler(events, async (_, ct) =>
            {
                await Task.Delay(Timeout.Infinite, ct); // wait for cancellation
                return "ok";
            });

        var client = ctx.CreateInvokeClient(events);
        var task = client.InvokeAsync(42, cts.Token);

        cts.Cancel();

        var act = () => task;
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task Should_HandleConcurrentInvokes()
    {
        var ctx = EventContext.Create();
        var events = new InvokeEventDefinition<int, int>();

        using var handler = ctx.RegisterInvokeHandler(events, (val, _) => Task.FromResult(val * 2));
        var client = ctx.CreateInvokeClient(events);

        var results = await Task.WhenAll(
            client.InvokeAsync(10, default),
            client.InvokeAsync(20, default),
            client.InvokeAsync(50, default));

        results.Should().Equal(20, 40, 100);
    }
}
```

#### Stream Tests

```csharp
public class StreamTests
{
    [Fact]
    public async Task Should_StreamServerResponses()
    {
        var ctx = EventContext.Create();
        var events = new InvokeEventDefinition<ProgressOrResult, JobRequest>();

        using var handler = ctx.RegisterStreamHandler(events, async (req, ct) => ServerStreamImpl(req, ct));
        var client = ctx.CreateInvokeStreamClient(events);

        var results = new List<ProgressOrResult>();
        await foreach (var item in client.InvokeAsync(new JobRequest("alice"), default))
        {
            results.Add(item);
        }

        results.Should().HaveCount(6); // 5 progress + 1 result
    }

    private static async IAsyncEnumerable<ProgressOrResult> ServerStreamImpl(
        JobRequest req,
        [EnumeratorCancellation] CancellationToken ct)
    {
        for (var i = 1; i <= 5; i++)
        {
            yield return new ProgressOrResult.Progress(i * 20);
        }

        yield return new ProgressOrResult.Result(true);
    }
}
```

---

## 6. Recommended Project Structure

```
Eventa.sln
├── src/
│   ├── Eventa.Core/                     # Core library
│   │   ├── EventDefinition.cs           # EventDefinition<T> and InvokeEventDefinition<TResponse, TRequest>
│   │   ├── EventContext.cs              # IEventContext implementation
│   │   ├── EventInvoke.cs               # CreateInvokeClient / RegisterInvokeHandler
│   │   ├── EventStream.cs               # CreateInvokeStreamClient / RegisterStreamHandler
│   │   ├── MatchExpression.cs           # matchBy / and / or
│   │   ├── IEventaAdapter.cs            # adapter interface
│   │   └── IdGenerator.cs               # nanoid equivalent
│   │
│   ├── Eventa.Adapters.WebSocket/       # WebSocket adapter
│   ├── Eventa.Adapters.SignalR/         # SignalR adapter
│   ├── Eventa.Adapters.Channels/        # System.Threading.Channels adapter
│   └── Eventa.Adapters.Grpc/            # gRPC adapter
│
├── tests/
│   ├── Eventa.Core.Tests/               # Core unit tests
│   │   ├── EventContextTests.cs
│   │   ├── InvokeTests.cs
│   │   ├── StreamTests.cs
│   │   └── MatchExpressionTests.cs
│   │
│   └── Eventa.Adapters.Tests/           # Adapter integration tests
│
└── samples/
    ├── Eventa.Sample.Console/           # Console sample
    └── Eventa.Sample.WebApi/            # ASP.NET Core sample
```

**NuGet package split**:

| Package | Contents | Dependencies |
|------|------|------|
| `Eventa.Core` | Event definitions, Context, Invoke, Stream | No external dependencies |
| `Eventa.Adapters.WebSocket` | WebSocket adapter | `Eventa.Core` |
| `Eventa.Adapters.SignalR` | SignalR adapter | `Eventa.Core` + `Microsoft.AspNetCore.SignalR` |
| `Eventa.Adapters.Grpc` | gRPC adapter | `Eventa.Core` + `Grpc.Net.Client` |

---

## 7. Risks and Differences

Note: this section describes migration-time risks and design-level differences.
For differences verified against the current playground C# implementation, see
`docs/eventa-csharp-known-differences.md`.

### 7.1 TS Features Without a Direct Equivalent

| TS feature | Impact | C# alternative |
|------|------|------|
| Conditional types (`T extends X ? Y : Z`) | Invoke signatures can make parameters optional when `Req` is `undefined` | Provide multiple overloads (`Invoke()` and `Invoke(TReq req)`) |
| `Transferable` / Structured Clone | `withTransfer()` has no direct meaning | Not needed: C# cross-process communication already uses serialization |
| Glob matching (`picomatch`) | `matchBy("pattern*")` | Use `Microsoft.Extensions.FileSystemGlobbing` or regex |
| Function-stub serialization | `withRemoteMethods()` serializes functions into markers | **Lower priority**: delegates are not serializable in C# and need a different design such as named service registration |

### 7.2 C#-Specific Concerns That Must Be Addressed

| Concern | Explanation | Recommendation |
|------|------|------|
| **Thread safety** | TS is single-threaded; C# must handle concurrent access | Use `ConcurrentDictionary` plus `lock`/`ReaderWriterLockSlim` where needed |
| **Memory leaks** | TS relies on GC and closures; C# event subscriptions are strong references | `Subscribe()` should return `IDisposable`; encourage `using` |
| **Exception propagation** | TS catches everything uniformly; C# distinguishes `Exception` and `OperationCanceledException` | Emit handler failures through protocol error events; preserve `OperationCanceledException` for cancellation |
| **Serialization** | TS uses JSON / Structured Clone; C# must choose explicitly | Default to `System.Text.Json`; let adapters plug in `IEventaSerializer` |
| **Performance** | `Channel<T>` can outperform TS `ReadableStream` for high-throughput buffering | Use `BoundedChannelOptions` to control backpressure |

### 7.3 Areas Not Worth Migrating Directly

- **Function Stub (`withRemoteMethods`)**: lower priority. TS can do this because
  functions are first-class and easy to wrap; C# delegates are not portable
  across processes. If needed later, prefer a named-service registration model.
- **`withTransfer`**: Structured Clone transfer is browser-specific and not
  needed in .NET.
- **Electron adapter**: specific to the JavaScript ecosystem.

---

## 8. Recommended Implementation Roadmap

### Phase 1 - Core Library (`Eventa.Core`)

1. Define `EventDefinition<T>` / `InvokeEventDefinition<TRes, TReq>` as records
2. Implement `EventContext` (in-memory loopback, Emit/Subscribe/SubscribeOnce/Unsubscribe)
3. Implement `CreateInvokeClient` / `RegisterInvokeHandler` (including
   `CancellationToken` and streaming request input)
4. Implement `CreateInvokeStreamClient` / `RegisterStreamHandler`
   (`IAsyncEnumerable`)
5. Implement `MatchExpression`, `And()`, and `Or()`
6. Port the full unit-test matrix from the TS specs

### Phase 2 - Adapters

1. WebSocket adapter (`ClientWebSocket` + ASP.NET Core middleware)
2. Channel adapter (cross-thread pipe)
3. SignalR adapter

### Phase 3 - Advanced Features

1. Context extensions (`Extensions` dictionary + abort-event registration)
2. `toStreamHandler` helper
3. Global metadata (`metadata` / `invokeMetadata`)
4. Serializer abstraction (`IEventaSerializer`)

---

## 9. Conclusion

**The migration is fully feasible.** Eventa's core design
(event definitions -> Context publish/subscribe -> 7-event invoke/stream
protocol -> adapter hooks) maps naturally onto stronger and more idiomatic
primitives in C# .NET 10:

- `record` replaces plain-object event definitions with immutable value types
- `IDisposable` replaces `() => void` unsubscribe functions and integrates with
  `using`
- `Task<T>` / `ValueTask<T>` replace `Promise<T>` with native async/await support
- `CancellationToken` replaces `AbortSignal` with a mature cancellation model
- `IAsyncEnumerable<T>` replaces `AsyncGenerator` / `ReadableStream` as a
  first-class async-stream abstraction
- `Channel<T>` can provide high-performance async buffering instead of manually
  managed `ReadableStream`
- Thread safety can be handled cleanly through `ConcurrentDictionary`,
  `Channel`, and `lock`

The core library is likely to land around 800-1200 lines of code excluding
tests, which is roughly in the same size range as the TS implementation.
