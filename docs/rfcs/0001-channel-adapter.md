# RFC 0001: Channel Adapter

Status: proposed

Last reviewed: 2026-05-10

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

Because adapter packages are intended to live outside the core package, inbound
dispatch and transport fatal notification surfaces must be public core
abstractions. Channel adapters must not depend on `EventContext` internals.

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
- No merger, multiplexer, router, hub, or broadcast API in v1.
- No high-throughput backpressure guarantee in the default `ChannelPipe`.
- No reflection-based serializer or dynamic dispatch path.

## Requirement Layers

This RFC is intentionally a single-file audit ledger. Requirements are layered
so the primary contract remains readable while edge-case decisions stay
recorded:

- **Must**: public API and behavior required for v1.
- **Should**: preferred behavior that keeps v1 extensible without overfitting
  the implementation.
- **Audit Notes**: race, ordering, and negative-space decisions captured to
  prevent regressions or contradictory future edits.
- **Verification Criteria**: reviewer-facing acceptance coverage;
  implementation may split criteria into smaller tests when that improves
  diagnostics.

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

`ChannelPipe` is the owner for one connected pair. `Left` and `Right` are the
two symmetric endpoint objects of that pair, not a topology abstraction for
fan-in, fan-out, routing, or broadcast. Each `ChannelEndpoint` is directly
usable as an `IEventContext`. This follows the .NET pattern where a
connection-facing object can own transport state and lifecycle while also
exposing the primary operations for that endpoint.

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

    public ChannelPipe(ChannelPipeOptions? options = null);

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

public sealed class ChannelPipeOptions
{
    public EventDefinition<ChannelClosedPayload> ClosedEvent { get; init; }
        = ChannelEvents.Closed;
}

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

`ChannelPipe` owns the internal channels that connect `Left` and `Right`.
Pipe-created endpoints always own their internal outbound writers and complete
them on disposal so the paired endpoint observes channel completion. This writer
ownership is not configurable through `ChannelPipeOptions`. `ChannelPipeOptions`
applies `ClosedEvent` symmetrically to both pipe-created endpoints, `Left` and
`Right`.

Custom `ChannelEndpoint` instances respect `CompleteOutboundOnDispose`; when it
is `true`, disposal completes the externally supplied outbound writer, and when
it is `false`, writer completion is left to the external owner.

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

public interface IEventTransportFatalNotifier
{
    void NotifyTransportFatal(Exception error);
}
```

`EventContext` should implement `IEventContext`, `IEventInboundDispatcher`, and
`IEventTransportFatalNotifier`. `ChannelEndpoint` implements `IEventContext` by
forwarding context operations to an owned `EventContext`; the endpoint keeps its
own `IEventInboundDispatcher` reference for the inbound pump and its own
`IEventTransportFatalNotifier` reference for terminal transport notification.

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
- one `IEventTransportFatalNotifier` reference for transport fatal notification
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
notify invoke session infrastructure directly before the public closed event is
dispatched. It must not depend on normal closed-event listener enumeration
order.

The fatal mapping should preserve `ChannelClosedPayload.Error` when present and
otherwise use a `ChannelClosedException`.

`IEventInboundDispatcher` handles successful inbound envelopes.
`IEventTransportFatalNotifier` handles terminal transport failure.

## Core Inbound Dispatch Requirement

The core package must expose `IEventInboundDispatcher.Receive`. It is the stable
package boundary used by external adapter packages.

### Must

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

### Audit Notes

This requires a new boxed dispatch primitive below `EventContext`. The current
`EventListenerStore.CreateDispatchSnapshot<TPayload>` constructs a new
`EventEnvelope<TPayload>` from a payload; remote input needs a sibling primitive
that validates and dispatches an already-created boxed envelope.

## Core Transport Fatal Notification Requirement

The core package must expose
`IEventTransportFatalNotifier.NotifyTransportFatal`. It is the stable public
package boundary used by external adapter packages to fault invoke sessions when
the transport becomes terminal.

The notifier is not responsible for public closed-event dispatch. It faults
pending unary and active stream sessions only; `ChannelEndpoint` owns the
best-effort public closed event after notifier delivery.

### Must

- Accept the adapter-mapped terminal exception.
- Notify pending unary invoke sessions directly.
- Notify active stream invoke sessions directly and surface the same exception
  through their async enumerables.
- Preserve the per-session first-wins rule.
- Never call user `Emit`, direct listeners, match listeners, registered
  `RegisterAbortEvent` fatal-event subscriptions, or the configured public
  closed event.
- Never depend on listener enumeration order.

### Should

- Remain transport-generic core/invoke infrastructure that channel, WebSocket,
  and SignalR adapters can reuse.

### Audit Notes

This separates deterministic session failure from ordinary event observation.
The public closed event remains observable, but it is endpoint-owned and runs
after the notifier.

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

### Must

When an endpoint observes a transport fatal condition, it must:

- enter terminal state
- notify invoke session infrastructure through the deterministic transport fatal
  path
- fault pending unary invoke sessions before public closed-event listeners run
- fault active stream invoke sessions before public closed-event listeners run
- surface the same exception through stream async enumeration
- best-effort dispatch the configured closed event for user observers
- do not emit client abort protocol events, because the failure did not come
  from local cancellation or consumer disposal

`ChannelEndpoint` owns the public closed event. The notifier only faults
sessions; endpoint terminal handling dispatches the configured closed event
after notifier delivery.

### Should

- Normal channel completion maps to a `ChannelClosedException`.
- Local endpoint disposal maps to
  `ChannelClosedException("Channel endpoint disposed.")`.
- Faulted channel completion preserves the original channel exception, wrapping
  it only when a public `ChannelClosedException` is needed to add context. When
  wrapped, the original exception is exposed as `InnerException`.

### Audit Notes

- Stream invoke sessions must observe fatal transport events. The current stream
  behavior that ignores fatal abort registrations is not compatible with this
  adapter's v1 streaming scope.
- Endpoint terminal cause precedence is first terminal cause wins. The first
  remote close, remote fault, local endpoint disposal, malformed or null
  envelope, inbound dispatch rejection, or inbound listener exception determines
  the mapped fatal exception, `ChannelClosedPayload.Error`, and cleanup sequence.
- Later terminal causes are ignored for public closed-event dispatch and invoke
  or stream fatal notification.
- Session result precedence is first terminal result wins. If per-call
  cancellation, a protocol response, a protocol error, or a stream end has
  already completed a unary or stream session, transport fatal notification must
  not rewrite that result. Otherwise the mapped transport exception faults the
  session.

The deterministic transport fatal path is not normal event dispatch. It must not
rely on `HashSet` listener order, arbitrary user callbacks, or public
closed-event listener enumeration. The channel adapter calls the public
`IEventTransportFatalNotifier.NotifyTransportFatal` boundary with the mapped
terminal exception. The existing `RegisterAbortEvent` public API can continue to
exist for ordinary fatal-event observation, but it is not the deterministic
transport fatal delivery path.

## Lifecycle

### Must

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

Ordering:

- Deterministic transport fatal notification must run before public closed-event
  dispatch and before `EventContext.Dispose()` clears listeners.
- Public closed-event listener failures must not prevent pending unary or active
  stream sessions from faulting.
- Thread safety must not weaken this ordering: the winning disposer runs the
  sequence, and concurrent disposers observe completion or no-op.

For a remote close or fault, the receiving endpoint's inbound pump must:

1. stop reading further messages
2. enter terminal state
3. fault pending unary and active stream invokes through deterministic transport
   fatal notification
4. best-effort dispatch the configured closed event through inbound dispatch
5. dispose endpoint resources

For local endpoint disposal, the endpoint must:

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

### Should

When outbound completion is owned, or when the external owner completes or faults
the writer, the paired endpoint observes disposal through channel completion and
uses the remote close or fault sequence above. If `CompleteOutboundOnDispose` is
`false` and no external completion or fault occurs, the paired endpoint is not
guaranteed to observe local endpoint disposal.

### Audit Notes

Post-terminal user `IEventContext` operations are an audit contract, not the
primary API story:

- User-initiated `Emit`, `Subscribe`, `SubscribeOnce`, `Unsubscribe`, invoke
  handler registration, and stream handler registration after endpoint terminal
  transition must fail fast with `ChannelClosedException`.
- `CreateInvokeClient` and `CreateInvokeStreamClient` remain pure reusable client
  factories and may succeed after endpoint terminal transition. The first unary
  or stream invoke use against a terminal endpoint must fail fast with
  `ChannelClosedException`, before local dispatch or outbound channel writes.
- User-initiated `Emit` after endpoint terminal transition must not run local
  listener dispatch and must not write to the outbound channel.
- Internal deterministic fatal notification and public closed-event dispatch are
  allowed during the terminal sequence. They are not routed through ordinary
  user-initiated `Emit`.

## Error Handling

### Must

Outbound write failure:

- If `OnSent` cannot write because the underlying `System.Threading.Channels`
  writer is closed or faulted, it should propagate that writer failure unchanged
  to the local `Emit` caller. This is distinct from the adapter
  `ChannelClosedException` used for mapped terminal state and post-terminal user
  operations.
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

### Audit Notes

- Disposal is not considered a malformed message.
- Disposal should stop the inbound pump and optionally complete outbound.

## Backpressure

`ChannelPipe` v1 uses unbounded channels by default. This matches the current
in-process, fire-and-forget Eventa dispatch model and keeps the first adapter
focused on transport semantics rather than throughput policy.

The v1 API does not promise high-throughput backpressure behavior. Bounded
channels and explicit write-pressure options can be added later without changing
the constructor-first shape.

## Verification Criteria

The RFC keeps reviewer-facing verification criteria grouped by behavior.
Implementation may split these rows into smaller test cases when that improves
diagnostics.

- Transport smoke: ordinary events, unary invoke, request-stream unary invoke,
  server-streaming invoke, and bidirectional streaming invoke cross from `Left`
  to `Right`.
- Inbound dispatch: remote receive dispatches direct, match-expression, and
  one-shot listeners; reuses the original envelope; calls `OnReceived`; never
  calls `OnSent`; does not echo-loop; rejects null, mismatched, or invalid
  envelope shapes.
- Core boundaries: external adapter-facing code can use
  `IEventInboundDispatcher` and `IEventTransportFatalNotifier` without core
  internals; boxed inbound dispatch avoids reflection and dynamic invocation.
- Fatal path: channel close, channel fault, local endpoint disposal, malformed
  input, and inbound listener exceptions fault pending unary invokes and active
  stream async enumerables through the deterministic notifier before public
  closed-event dispatch or context disposal.
- Fault mapping: normal completion and local disposal surface
  `ChannelClosedException`; local disposal uses
  `ChannelClosedException("Channel endpoint disposed.")`; faulted channels
  preserve the original exception or wrap it with that exception as
  `InnerException`.
- Lifecycle: disposing one pipe-created endpoint completes its owned outbound
  writer, so the paired endpoint observes channel completion; custom endpoint
  disposal respects `CompleteOutboundOnDispose=true` and `false`; concurrent
  disposal and disposal through `IEventContext` are idempotent, do not
  double-notify, and stop inbound loops.
- Post-terminal operations: user-initiated `Emit`, listener registration,
  unsubscription, and handler registration fail fast with
  `ChannelClosedException`; reusable invoke client factories may still be
  created, but first invoke use fails fast before local dispatch or outbound
  writes.
- Race audit: endpoint terminal cause is first-wins; per-session result is
  first-wins across transport fatal, cancellation, protocol response, protocol
  error, and stream end.
- Closed event: public closed event remains best-effort observable; closed-event
  listener failures and listener ordering never affect invoke or stream
  termination.
- Backpressure and AOT: default `ChannelPipe` uses unbounded channels and makes
  no high-throughput backpressure guarantee; `Eventa.Adapters.Channels` sets
  `IsAotCompatible=true`; core and adapter AOT/trim analyzer builds complete
  without IL warnings.
- Documentation: README covers constructor-first pair usage, in-process
  object-only scope, and unbounded-channel/backpressure limits.

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

### Routing, Merger, Multiplexer, and Broadcast API

Deferred for v1. `ChannelPipe` is the minimal point-to-point full-duplex
transport: one pair, two endpoints, and no routing policy.

Fan-in, fan-out, multiplexing, and broadcast are composition layers above the
pair transport. They need separate semantics for ordering, backpressure, loop
prevention, endpoint identity, duplicate delivery, and failure fan-out. The
custom `ChannelEndpoint(ChannelReader<ChannelMessage>,
ChannelWriter<ChannelMessage>, ChannelEndpointOptions?)` constructor remains
the low-level escape hatch for experiments or future APIs such as a
`ChannelRouter`, `ChannelHub`, or broadcast adapter.

### Serialization in v1

Rejected. The first adapter should validate Eventa's transport boundary without
also designing serializer policy. Serialization should be introduced with a
transport that requires it.

### Adapter Depending on Core Internals

Rejected. Adapter packages are intended to be separate from the core package, so
the inbound transport boundary must be public and stable.

## Open Questions

None.
