# RFC 0001: Channel Adapter

Status: proposed

Last reviewed: 2026-05-09

## Summary

Add a C# `Eventa.Adapters.Channels` adapter that connects two Eventa contexts
with `System.Threading.Channels`. The v1 adapter is an in-process object
transport for validating cross-context event, invoke, and stream behavior before
network transports are introduced.

The public API should be constructor-first. The primary convenience type is
`ChannelPipe`, not a static `ChannelEventContext.Create*` factory.

## Problem

The current core `IEventaAdapter` surface observes local activity through
`OnSent` and `OnReceived`. That is enough for recording hooks, but not enough
for a real transport:

- `OnSent` can forward a locally emitted envelope to a remote endpoint.
- The remote endpoint still needs a stable inbound dispatch surface.
- Calling the normal `Emit` path for remote input would notify `OnSent` again
  and can create an echo loop.

Because adapter packages are intended to live outside the core package, the
inbound dispatch surface must be a public core abstraction. Channel adapters
must not depend on `EventContext` internals.

## Goals

- Provide an in-memory object transport using `System.Threading.Channels`.
- Support ordinary events, unary invoke, request-stream unary invoke, server
  streaming, and bidirectional streaming across two contexts.
- Use a constructor-first API that reads like normal C# object composition.
- Keep v1 typed-object only; no JSON serialization or serializer registry.
- Keep the transport symmetric; do not force client/server terminology.
- Make channel close and channel fault observable by pending unary invokes and
  active stream invokes.
- Keep the core additions and `Eventa.Adapters.Channels` AOT-compatible.

## Non-Goals

- No network transport.
- No JSON or binary serialization.
- No cross-process payload compatibility.
- No dependency-injection or hosted-service API.
- No static factory class as the primary public API.
- No `Client` / `Server` naming for the in-memory pair.
- No high-throughput backpressure guarantee in the default `ChannelPipe`.
- No reflection-based serializer or dynamic dispatch path.

## Public API

Namespace:

```csharp
namespace Eventa.Adapters.Channels;
```

Create a connected in-memory pair:

```csharp
using Eventa;
using Eventa.Adapters.Channels;

using var pipe = new ChannelPipe();

IEventContext left = pipe.Left;
IEventContext right = pipe.Right;
```

`ChannelPipe` is the owner for the connected pair. `Left` and `Right` are
symmetric endpoint objects, and each `ChannelEndpoint` is directly usable as an
`IEventContext`. This follows the .NET pattern where a connection-facing object
can own transport state and lifecycle while also exposing the primary operations
for that endpoint.

Use a custom channel endpoint:

```csharp
using System.Threading.Channels;
using Eventa;
using Eventa.Adapters.Channels;

var inbound = Channel.CreateUnbounded<ChannelMessage>();
var outbound = Channel.CreateUnbounded<ChannelMessage>();

using var endpoint = new ChannelEndpoint(
    inbound.Reader,
    outbound.Writer,
    new ChannelEndpointOptions
    {
        CompleteOutboundOnDispose = true,
    });

IEventContext context = endpoint;
```

Recommended public channel types:

```csharp
public sealed class ChannelPipe : IDisposable
{
    public ChannelEndpoint Left { get; }

    public ChannelEndpoint Right { get; }

    public ChannelPipe(ChannelEndpointOptions? options = null);

    public void Dispose();
}

public sealed class ChannelEndpoint : IEventContext
{
    // IEventContext members are forwarded to the owned EventContext.

    public ChannelEndpoint(
        ChannelReader<ChannelMessage> inbound,
        ChannelWriter<ChannelMessage> outbound,
        ChannelEndpointOptions? options = null);

    public void Dispose();
}

public sealed record ChannelMessage(
    IEventEnvelope Envelope,
    object? Options = null);

public sealed class ChannelEndpointOptions
{
    public bool CompleteOutboundOnDispose { get; init; }

    public EventDefinition<ChannelClosedPayload> ClosedEvent { get; init; }
        = ChannelEvents.Closed;
}

public static class ChannelEvents
{
    public static EventDefinition<ChannelClosedPayload> Closed { get; }
        = new("eventa:channels:closed");
}

public sealed record ChannelClosedPayload(Exception? Error);

public sealed class ChannelClosedException : Exception
{
    public ChannelClosedException(string message);

    public ChannelClosedException(string message, Exception innerException);
}
```

`ChannelPipe` owns the internal channels that connect `Left` and `Right`; both
pipe-created endpoints must complete their outbound writers on disposal so the
paired endpoint observes channel completion. Custom `ChannelEndpoint` instances
respect `CompleteOutboundOnDispose`; when it is `false`, disposing the endpoint
does not complete an externally owned outbound writer.

Recommended new core transport-facing abstractions:

```csharp
public interface IEventEnvelope
{
    string EventId { get; }

    Type PayloadType { get; }

    object? UntypedBody { get; }
}

public sealed record EventEnvelope<TPayload>(
    string EventId,
    TPayload Body) : IEventEnvelope
{
    public Type PayloadType => typeof(TPayload);

    object? IEventEnvelope.UntypedBody => Body;
}

public interface IEventInboundDispatcher
{
    void Receive(IEventEnvelope envelope, object? options = null);
}
```

`EventContext` should implement both `IEventContext` and
`IEventInboundDispatcher`. `ChannelEndpoint` implements `IEventContext` by
forwarding context operations to an owned `EventContext`; the endpoint keeps its
own `IEventInboundDispatcher` reference for the inbound pump.

## Internal Architecture

`ChannelPipe` owns two unbounded channels and wires two endpoints in opposite
directions:

```text
Left
  Emit
    -> local dispatch
    -> ChannelAdapter.OnSent
    -> leftToRight.Writer
    -> Right inbound loop
    -> Right inboundDispatcher.Receive
    -> right local dispatch + OnReceived only

Right follows the same path through rightToLeft.
```

`ChannelEndpoint` owns:

- one inbound `ChannelReader<ChannelMessage>`
- one outbound `ChannelWriter<ChannelMessage>`
- one owned `EventContext` used to implement `IEventContext`
- one `IEventInboundDispatcher` reference for remote input
- one private `ChannelAdapter`
- one background inbound pump task
- one cancellation source for endpoint disposal

`ChannelEndpoint` intentionally keeps transport lifecycle outside the owned
`EventContext`. It forwards `IEventContext` operations to that context, but
endpoint disposal, inbound-pump cancellation, channel completion, and transport
fatal notification remain endpoint responsibilities.

`ChannelAdapter` implements `IEventaAdapter`:

- `OnSent` writes `ChannelMessage(envelope, options)` to the outbound writer.
- `OnReceived` does not forward anything; it remains an observation hook.
- `Dispose` stops accepting new outbound sends.

The inbound pump reads messages from the inbound reader and dispatches each
message through the remote inbound path:

```csharp
await foreach (var message in inbound.ReadAllAsync(cancellationToken))
{
    inboundDispatcher.Receive(message.Envelope, message.Options);
}
```

`ChannelEndpoint` also uses a deterministic transport fatal notification path
for channel close, channel fault, and local endpoint disposal. This path must
notify invoke internals directly before the public closed event is dispatched.
It must not depend on normal closed-event listener enumeration order.

The fatal mapping should preserve `ChannelClosedPayload.Error` when present and
otherwise use a `ChannelClosedException`.

## Core Inbound Dispatch Requirement

The core package must expose `IEventInboundDispatcher.Receive`. It is the stable
package boundary used by external adapter packages.

Required behavior:

- Dispatch from the existing `IEventEnvelope` instance without constructing a
  new envelope.
- Use `IEventEnvelope.EventId` as the dispatch key.
- Dispatch to direct listeners registered for that event id.
- Dispatch to matching `MatchExpression<TPayload>` listeners.
- Invoke adapter `OnReceived` for each selected direct or match listener.
- Never invoke adapter `OnSent`.
- Preserve existing one-shot listener semantics.
- Preserve existing payload-type binding diagnostics.
- Reject null envelopes.
- Reject envelopes whose `PayloadType` conflicts with the context binding for
  the event id or matching expression id.
- Reject envelopes whose runtime shape cannot be used as
  `EventEnvelope<TPayload>` for the selected payload type.

This requires a new boxed dispatch primitive below `EventContext`. The current
`EventListenerStore.CreateDispatchSnapshot<TPayload>` constructs a new
`EventEnvelope<TPayload>` from a payload; remote input needs a sibling primitive
that validates and dispatches an already-created boxed envelope.

## AOT Compatibility

`Eventa.Adapters.Channels` must follow the same AOT stance as the core `Eventa`
package. The adapter project should set `IsAotCompatible=true`, and the core
changes required by this RFC must continue to build cleanly with trim and AOT
analyzers enabled.

The forbidden APIs below are excluded because trim and AOT analysis cannot
reliably preserve runtime reflection, runtime generic construction, runtime
activation, or dynamic dispatch. The inbound dispatch design should expose the
needed typed delegates ahead of time instead of reconstructing type-specific
behavior at runtime.

The boxed inbound dispatch path must be implemented without:

- `dynamic`
- `Delegate.DynamicInvoke`
- `MethodInfo.Invoke`
- `MakeGenericMethod`
- `Activator.CreateInstance`
- reflection-based serializer calls
- IL warning suppressions such as `#pragma warning disable IL*` or
  `UnconditionalSuppressMessage`

`IEventEnvelope.PayloadType` is allowed for exact type identity checks,
payload-binding validation, and diagnostics. It must not be used to discover
members, construct generic methods, or instantiate payload types at runtime.

The dispatch primitive should remain non-reflective by reusing typed delegates
captured at subscription time or by storing explicit erased dispatch delegates
inside the listener store. Any annotation needed for `Type` flow must be carried
through public and internal APIs rather than hidden behind suppressions.

## Message Shape

`ChannelMessage` carries one already-created typed envelope:

```csharp
public sealed record ChannelMessage(
    IEventEnvelope Envelope,
    object? Options = null);
```

There is no outer `EventId`. `IEventEnvelope.EventId` is the single source of
truth. This avoids ambiguity between a message-level id and an envelope-level id.

For v1, `Envelope` is expected to be an `EventEnvelope<TPayload>` instance from
the sending context. This keeps the channel adapter in-process only and avoids
inventing a serializer contract before WebSocket or SignalR forces one.

`Options` is forwarded for future adapter metadata symmetry, but v1 does not
require any built-in option type.

## Transport Failure Semantics

Channel close, channel fault, and local endpoint disposal are transport fatal
conditions.

When an endpoint observes a transport fatal condition:

- enter terminal state
- notify invoke internals through the deterministic transport fatal path
- fault pending unary invoke sessions before public closed-event listeners run
- fault active stream invoke sessions before public closed-event listeners run
- surface the same exception through stream async enumeration
- best-effort dispatch the configured closed event for user observers
- do not emit client abort protocol events, because the failure did not come
  from local cancellation or consumer disposal

This requires stream invoke sessions to observe fatal transport events. The
current stream behavior that ignores fatal abort registrations is not compatible
with this adapter's v1 streaming scope.

Normal channel completion should map to a `ChannelClosedException`. Local
endpoint disposal should map to
`ChannelClosedException("Channel endpoint disposed.")`. Faulted channel
completion should preserve the original channel exception, wrapping it only when
a public `ChannelClosedException` is needed to add context. If a faulted
completion is wrapped, the original exception must be exposed as the wrapper's
`InnerException`.

Endpoint terminal cause precedence is first terminal cause wins. The first
remote close, remote fault, local endpoint disposal, malformed or null envelope,
inbound dispatch rejection, or inbound listener exception that moves the endpoint
into terminal state determines the mapped fatal exception,
`ChannelClosedPayload.Error`, and cleanup sequence. Later terminal causes are
ignored for public closed-event dispatch and invoke or stream fatal notification.

Terminal result precedence is first terminal result wins for every transport
fatal condition. The endpoint first-wins rule selects the transport fatal
exception; the session first-wins rule decides whether that exception can still
fault each pending unary or stream session. If per-call cancellation, a protocol
response, a protocol error, or a stream end has already completed a unary or
stream session, transport fatal notification must not rewrite that result.
Otherwise channel close, channel fault, local endpoint disposal, malformed input,
or inbound listener failure faults the session with the mapped transport
exception.

The deterministic transport fatal path is not normal event dispatch. It must not
rely on `HashSet` listener order, arbitrary user callbacks, or public
closed-event listener enumeration. The existing `RegisterAbortEvent` public API
can continue to exist, but the channel adapter needs a lower-level core helper
or invoke-internal hook that can notify pending unary and active stream sessions
directly. That helper or hook must be transport-generic core/invoke
infrastructure, not a channel-specific patch, so later WebSocket or SignalR
adapters can reuse the same fatal path.

## Lifecycle

`ChannelPipe.Dispose()`:

- disposes `Left`
- disposes `Right`
- is idempotent and thread-safe under concurrent calls
- does not double-run endpoint terminal work when `ChannelPipe.Dispose()`,
  `Left.Dispose()`, and `Right.Dispose()` race with each other

`ChannelEndpoint.Dispose()`:

- treats local endpoint disposal as a local transport terminal condition
- stops accepting new outbound sends
- completes the outbound writer when `CompleteOutboundOnDispose` is `true`
- cancels the inbound pump for local disposal
- runs deterministic transport fatal notification before disposing the context
- best-effort dispatches the configured closed event before disposing the context
- disposes the owned `EventContext` only after transport-close notification has
  had a chance to reach local listeners
- runs the same endpoint disposal path when disposed through the `IEventContext`
  interface
- does not block indefinitely waiting for remote code
- is idempotent and thread-safe under concurrent calls
- runs the terminal transition, optional outbound completion, inbound pump
  cancellation, deterministic transport fatal notification, closed-event
  dispatch, and owned `EventContext` disposal at most once

Ordering rule:

Deterministic transport fatal notification must run before public closed-event
dispatch and before `EventContext.Dispose()` clears listeners. Public
closed-event listener failures must not prevent pending unary or active stream
sessions from faulting. Thread safety must not weaken this ordering: the winning
disposer runs the sequence, and concurrent disposers observe completion or no-op.

For a remote close or fault, the receiving endpoint's inbound pump should:

1. stop reading further messages
2. enter terminal state
3. fault pending unary and active stream invokes through deterministic transport
   fatal notification
4. best-effort dispatch the configured closed event through inbound dispatch
5. dispose endpoint resources

For local endpoint disposal, the endpoint should:

1. stop local outbound writes
2. complete outbound when configured to own completion
3. cancel its own inbound pump
4. enter terminal state with
   `ChannelClosedException("Channel endpoint disposed.")`
5. fault pending unary and active stream invokes through deterministic transport
   fatal notification
6. best-effort dispatch the configured closed event locally
7. dispose its context after fatal notification and closed-event dispatch have
   been attempted

Local endpoint disposal must not emit invoke `SendAbort` protocol events. It is
not a user cancellation of one invoke; it is endpoint ownership termination.
Terminal result precedence follows the transport-wide first-wins rule above.
Otherwise local endpoint disposal faults the session with
`ChannelClosedException("Channel endpoint disposed.")`.

When outbound completion is owned, or when the external owner completes or faults
the writer, the paired endpoint observes disposal through channel completion and
uses the remote close or fault sequence above. If `CompleteOutboundOnDispose` is
`false` and no external completion or fault occurs, the paired endpoint is not
guaranteed to observe local endpoint disposal.

Post-terminal user `IEventContext` operations:

- User-initiated `Emit`, `Subscribe`, `SubscribeOnce`, `Unsubscribe`, invoke
  handler registration, and stream handler registration after endpoint terminal
  transition must fail fast with `ObjectDisposedException` or
  `ChannelClosedException`.
- `CreateInvokeClient` and `CreateInvokeStreamClient` remain pure reusable client
  factories and may succeed after endpoint terminal transition. The first unary
  or stream invoke use against a terminal endpoint must fail fast with
  `ObjectDisposedException` or `ChannelClosedException`, before local dispatch or
  outbound channel writes.
- User-initiated `Emit` after endpoint terminal transition must not run local
  listener dispatch and must not write to the outbound channel.
- Internal deterministic fatal notification and public closed-event dispatch are
  allowed during the terminal sequence. They are not routed through ordinary
  user-initiated `Emit`.

## Error Handling

Outbound write failure:

- If `OnSent` cannot write because the channel is closed, it should throw the
  channel write exception to the local `Emit` caller.
- Invoke clients should surface this as a local send failure, consistent with
  existing invoke session send-fault behavior.

Inbound malformed message:

- If the inbound pump receives a `ChannelMessage` with a null envelope, it should
  fault the endpoint.
- If inbound dispatch rejects an envelope because its event id, payload type, or
  runtime shape is invalid for the receiving context, the inbound pump should
  fault the endpoint.
- The endpoint should run deterministic transport fatal notification, then
  best-effort dispatch the configured closed event with that exception before
  cleanup.

Inbound listener exception:

- If a user listener throws during remote inbound dispatch, the exception faults
  the endpoint and stops the inbound pump.
- The endpoint should run deterministic transport fatal notification, then
  best-effort dispatch the configured closed event with the listener exception
  before cleanup.
- If a closed-event listener also throws, endpoint cleanup still proceeds and no
  additional closed event is attempted. Pending unary and active stream sessions
  must already have been faulted before any public closed-event listener runs.

Endpoint disposal:

- Disposal is not considered a malformed message.
- Disposal should stop the inbound pump and optionally complete outbound.

## Backpressure

`ChannelPipe` v1 uses unbounded channels by default. This matches the current
in-process, fire-and-forget Eventa dispatch model and keeps the first adapter
focused on transport semantics rather than throughput policy.

The v1 API does not promise high-throughput backpressure behavior. Bounded
channels and explicit write-pressure options can be added later without changing
the constructor-first shape.

## Testing

Core inbound dispatch tests:

- Remote receive dispatches ordinary direct listeners.
- Remote receive dispatches match-expression listeners.
- Remote receive removes one-shot listeners before callback invocation.
- Remote receive invokes `OnReceived`.
- Remote receive does not invoke `OnSent`.
- Remote receive rejects null envelopes.
- Remote receive rejects mismatched envelope payload types.
- Remote receive reuses the original envelope instance.
- Boxed inbound dispatch does not use reflection or dynamic invocation.

Channel adapter tests:

- Ordinary event emitted on `Left` is received by `Right`.
- Unary invoke client on `Left` can call handler registered on `Right`.
- Request-stream unary invoke crosses the pipe.
- Server-streaming invoke crosses the pipe.
- Bidirectional streaming invoke crosses the pipe.
- Remote inbound dispatch does not echo-loop between left and right.
- Disposing `ChannelPipe` disposes both endpoints.
- Disposing one endpoint stops its inbound loop.
- Disposing one endpoint faults local pending unary invokes with
  `ChannelClosedException("Channel endpoint disposed.")`.
- Disposing one endpoint faults local active stream async enumerables with
  `ChannelClosedException("Channel endpoint disposed.")`.
- Local endpoint disposal runs deterministic transport fatal notification before
  public closed-event dispatch and before context disposal clears listeners.
- Local endpoint disposal does not emit invoke `SendAbort` protocol events.
- Disposing a default `ChannelPipe` endpoint is observed by the paired endpoint
  through channel completion.
- Disposing a custom endpoint with `CompleteOutboundOnDispose=true` is observed by
  the paired endpoint through channel completion.
- Disposing a custom endpoint with `CompleteOutboundOnDispose=false` does not
  promise paired endpoint observation until the external writer owner completes
  or faults the writer.
- Concurrent `ChannelPipe.Dispose()` calls dispose both endpoints once.
- Concurrent `ChannelEndpoint.Dispose()` calls run endpoint terminal notification
  once.
- Concurrent `ChannelPipe.Dispose()` racing with `Left.Dispose()` or
  `Right.Dispose()` does not double-notify and does not throw.
- Disposing through `(IEventContext)pipe.Left` participates in the same
  thread-safe disposal gate.
- Concurrent disposal preserves ordering: deterministic transport fatal
  notification runs before public closed-event dispatch, and public closed-event
  dispatch runs before owned `EventContext` disposal.
- Post-terminal user `Emit` fails fast without local dispatch or outbound write.
- Post-terminal user `Subscribe`, `SubscribeOnce`, `Unsubscribe`, invoke handler
  registration, and stream handler registration fail fast.
- Post-terminal `CreateInvokeClient` and `CreateInvokeStreamClient` remain pure
  factories and do not require terminal-state probing.
- First unary invoke use after endpoint terminal transition fails fast before
  local dispatch or outbound write.
- First stream invoke use after endpoint terminal transition fails fast before
  local dispatch or outbound write.
- Remote fault racing local endpoint disposal exposes the winning terminal cause
  consistently through `ChannelClosedPayload.Error` and invoke or stream fatal
  notification.
- Malformed inbound envelope racing inbound listener exception exposes only one
  winning terminal cause.
- Faulting one channel runs deterministic transport fatal notification before
  public closed-event dispatch and before context disposal clears listeners.
- Pending unary invoke faults when the channel closes or faults.
- Active stream invoke faults its async enumerable when the channel closes or
  faults.
- Normal channel completion faults pending unary invokes and active stream async
  enumerables with `ChannelClosedException`.
- Faulted channel completion faults pending unary invokes and active stream async
  enumerables with the original exception, or with a `ChannelClosedException`
  whose `InnerException` is the original exception.
- Per-call cancellation that completes before remote close, remote fault, or
  local endpoint disposal keeps the existing cancellation result.
- Transport fatal notification that completes before per-call cancellation faults
  sessions with the mapped transport exception.
- Protocol response that completes before remote close, remote fault, or local
  endpoint disposal keeps the successful response result.
- Protocol error that completes before remote close, remote fault, or local
  endpoint disposal keeps the protocol error result.
- Stream end that completes before remote close, remote fault, or local endpoint
  disposal keeps the stream completion result.
- A closed-event user listener throwing does not prevent pending unary invokes
  from faulting.
- A closed-event user listener throwing does not prevent active stream async
  enumerables from faulting.
- Multiple closed-event user listeners and `HashSet` ordering do not affect
  invoke or stream termination.
- The deterministic transport fatal helper or hook is named and documented as
  transport-generic core/invoke infrastructure, not a `ChannelEndpoint`-specific
  API.
- The public closed event remains best-effort observable by ordinary
  subscribers.
- Malformed or null envelopes fault the endpoint.
- Inbound listener exceptions fault the endpoint.
- Default `ChannelPipe` uses unbounded channels and does not claim backpressure
  guarantees.
- `Eventa.Adapters.Channels` sets `IsAotCompatible=true`.
- AOT/trim analyzer builds for core and channel adapter complete without IL
  warnings.

## Documentation

Add a README section for the adapter after implementation. The example should
prefer the constructor-first pair:

```csharp
using var pipe = new ChannelPipe();

using var registration = pipe.Right.RegisterInvokeHandler(echo, HandleEcho);

var client = pipe.Left.CreateInvokeClient(echo);
var response = await client.InvokeAsync(new EchoRequest("eventa"));
```

Document that v1 is in-process and object-only. Users who need cross-process or
network behavior should wait for WebSocket or SignalR adapters.

Document that default `ChannelPipe` channels are unbounded and are intended for
local composition, tests, and same-process boundaries, not as a throughput or
backpressure policy.

## References

API shape:

- [ASP.NET Core `ConnectionContext`](https://learn.microsoft.com/en-us/dotnet/api/microsoft.aspnetcore.connections.connectioncontext?view=aspnetcore-10.0):
  a connection-facing object encapsulates an individual connection, transport,
  lifecycle, and extension points. This supports making `ChannelEndpoint`
  directly implement `IEventContext` instead of exposing a nested `.Context`
  property.
- [System.IO.Pipelines `IDuplexPipe`](https://learn.microsoft.com/en-us/dotnet/standard/io/pipelines#iduplexpipe):
  `IDuplexPipe` represents one side of a full-duplex connection. This supports
  `ChannelPipe.Left` and `ChannelPipe.Right` as symmetric endpoint sides rather
  than client/server roles.

AOT compatibility:

- [.NET Native AOT compatibility analyzers](https://learn.microsoft.com/en-us/dotnet/core/deploying/native-aot/#aot-compatibility-analyzers):
  `IsAotCompatible=true` enables trim, single-file, and AOT analyzers for
  libraries.
- [Introduction to AOT warnings](https://learn.microsoft.com/en-us/dotnet/core/deploying/native-aot/fixing-warnings):
  reflection and runtime code generation can be incompatible with Native AOT;
  avoiding those calls is the preferred design.
- [Fixing trim warnings](https://learn.microsoft.com/en-us/dotnet/core/deploying/trimming/fixing-warnings):
  the first recommended approach is eliminating reflection; source generators
  are recommended for common reflection scenarios.
- [`MethodInfo.MakeGenericMethod`](https://learn.microsoft.com/en-us/dotnet/api/system.reflection.methodinfo.makegenericmethod?view=net-10.0):
  annotated with `RequiresDynamicCode` and `RequiresUnreferencedCode`, which is
  why the boxed dispatch path forbids runtime generic method construction.
- [`Activator.CreateInstance`](https://learn.microsoft.com/en-us/dotnet/api/system.activator.createinstance?view=net-10.0):
  some overloads are annotated with `RequiresUnreferencedCode`, which is why the
  adapter avoids runtime activation.
- [System.Text.Json source generation](https://learn.microsoft.com/en-us/dotnet/standard/serialization/system-text-json/source-generation):
  reflection-based serialization can break Native AOT apps; source generation is
  the intended AOT-friendly direction for future serializer work.

## Alternatives Considered

### Static Context Factory

Rejected:

```csharp
var pair = ChannelEventContext.CreatePair();
```

This reads like a utility class, hides the transport object, and makes ownership
less clear. It also differs from the preferred C# object-composition style.

### Client / Server Pair Names

Rejected for `ChannelPipe`. A channel pair is symmetric and does not impose
application roles. `Left` and `Right` are role-neutral.

Transport-specific adapters such as WebSocket or SignalR may use client/server
terms later because those transports naturally expose those roles.

### Serialization in v1

Rejected. The first adapter should validate Eventa's transport boundary without
also designing serializer policy. Serialization should be introduced with a
transport that requires it.

### Adapter Depending on Core Internals

Rejected. Adapter packages are intended to be separate from the core package, so
the inbound transport boundary must be public and stable.

## Open Questions

None.
