# RFC 2: WebSocket Adapter

Status: proposed

RFC PR: not opened yet

Start date: 2026-05-31

Last reviewed: 2026-05-31

## Summary

Add a C# `Eventa.Adapters.WebSockets` adapter package that connects Eventa
contexts over text WebSocket messages. The v1 adapter supports a .NET client
endpoint based on `ClientWebSocket` and an ASP.NET Core per-peer endpoint based
on the accepted `System.Net.WebSockets.WebSocket`.

The v1 wire format should be compatible with the current TypeScript WebSocket
adapter. Payload serialization is explicit and AOT-friendly through a
`WebSocketEventCatalog`; the adapter must not use reflection-based
serialization or runtime type discovery.

## Problem

The channel adapter validates Eventa's transport boundary, but it is an
in-process object transport. A WebSocket adapter introduces two new requirements:

- Event envelopes must cross a process and language boundary as JSON.
- The receiver must know the CLR payload type before it can create an
  `EventEnvelope<TPayload>` for `IEventInboundDispatcher.Receive`.
- TypeScript and C# invoke protocol details are not identical, so the adapter
  must bridge the current wire shape without rewriting the core invoke engine.
- WebSocket close and error events must fault pending unary invokes and active
  stream invokes deterministically.

Because the core Eventa package remains transport-agnostic and AOT-oriented,
the WebSocket package must own JSON message parsing, protocol bridging,
transport lifecycle, and remote error mapping outside core internals.

## Goals

- Provide `Eventa.Adapters.WebSockets` as a separate package and assembly.
- Support ordinary events, unary invoke, request-stream unary invoke, server
  streaming, and bidirectional streaming across WebSocket peers.
- Support one .NET client endpoint and one ASP.NET Core per-peer endpoint model.
- Keep the WebSocket wire shape compatible with the current TypeScript adapter.
- Use explicit event and invoke registration through `WebSocketEventCatalog`.
- Use `JsonTypeInfo<T>` so apps can use System.Text.Json source generation.
- Bridge current TypeScript and C# invoke protocol differences inside the
  adapter layer.
- Make WebSocket close and error observable by pending unary invokes and active
  stream invokes before public closed-event listeners run.
- Keep the package compatible with trim and Native AOT analyzers.

## Non-Goals

- No global broadcast context in v1.
- No reconnect, retry, heartbeat, keepalive, or session resumption policy.
- No authentication, authorization, origin validation, or subprotocol
  negotiation API beyond exposing the underlying WebSocket setup surface.
- No binary frames or custom binary protocol.
- No SignalR, gRPC, HTTP long-polling, or SSE adapter.
- No custom user-supplied error codec in v1.
- No reflection-based serializer fallback.
- No automatic payload-type inference from listeners or handlers.
- No multiplexing multiple logical Eventa peers over one WebSocket.
- No high-throughput backpressure guarantee in v1.

## Requirement Layers

This RFC follows the channel adapter RFC style. Requirements are layered so the
primary v1 contract stays readable while implementation-sensitive decisions are
recorded:

- **Must**: public API and behavior required for v1.
- **Should**: preferred behavior that keeps the design extensible without
  expanding v1 scope.
- **Audit Notes**: compatibility, ordering, race, and negative-space decisions
  captured to prevent contradictory future edits.
- **Verification Criteria**: reviewer-facing acceptance coverage; tests may be
  split into smaller cases when that improves diagnostics.

## Public API

Namespace:

```csharp
namespace Eventa.Adapters.WebSockets;
```

Create a client endpoint from an existing `ClientWebSocket`:

```csharp
using System.Net.WebSockets;
using System.Text.Json.Serialization;

using Eventa;
using Eventa.Adapters.WebSockets;

var echo = new InvokeEventDefinition<EchoResponse, EchoRequest>("demo:ws:echo");

var catalog = new WebSocketEventCatalog()
    .RegisterInvoke(
        echo,
        AppJsonContext.Default.EchoResponse,
        AppJsonContext.Default.EchoRequest);

var socket = new ClientWebSocket();
await socket.ConnectAsync(new Uri("wss://example.test/events"), cancellationToken);

await using var endpoint = new WebSocketClientEndpoint(socket, catalog);
IEventContext context = endpoint;
```

Use the convenience connect helper when the caller wants the endpoint to own the
client socket:

```csharp
await using var endpoint = await WebSocketClientEndpoint.ConnectAsync(
    new Uri("wss://example.test/events"),
    catalog,
    cancellationToken: cancellationToken);
```

Create an ASP.NET Core per-peer endpoint from an accepted WebSocket:

```csharp
app.Map("/events", async httpContext =>
{
    if (!httpContext.WebSockets.IsWebSocketRequest)
    {
        httpContext.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }

    using var socket = await httpContext.WebSockets.AcceptWebSocketAsync();
    await using var endpoint = new WebSocketPeerEndpoint(socket, catalog);

    using var handler = endpoint.RegisterInvokeHandler(
        echo,
        static (EchoRequest request, CancellationToken _) =>
            Task.FromResult(new EchoResponse(request.Input.ToUpperInvariant())));

    await endpoint.Completion.WaitAsync(httpContext.RequestAborted);
});
```

Recommended public types:

```csharp
public sealed class WebSocketEventCatalog
{
    public WebSocketEventCatalog Register<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        JsonTypeInfo<TPayload> payloadTypeInfo);

    public WebSocketEventCatalog RegisterInvoke<TResponse, TRequest>(
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        JsonTypeInfo<TResponse> responseTypeInfo,
        JsonTypeInfo<TRequest> requestTypeInfo);
}

public sealed class WebSocketClientEndpoint :
    IEventContext,
    IDisposable,
    IAsyncDisposable
{
    public WebSocketClientEndpoint(
        ClientWebSocket socket,
        WebSocketEventCatalog catalog,
        WebSocketEndpointOptions? options = null);

    public static Task<WebSocketClientEndpoint> ConnectAsync(
        Uri uri,
        WebSocketEventCatalog catalog,
        WebSocketEndpointOptions? options = null,
        CancellationToken cancellationToken = default);

    public Task Completion { get; }

    // IEventContext members are forwarded to the owned EventContext.
}

public sealed class WebSocketPeerEndpoint :
    IEventContext,
    IDisposable,
    IAsyncDisposable
{
    public WebSocketPeerEndpoint(
        WebSocket socket,
        WebSocketEventCatalog catalog,
        WebSocketEndpointOptions? options = null);

    public Task Completion { get; }

    // IEventContext members are forwarded to the owned EventContext.
}

public sealed class WebSocketEndpointOptions
{
    public int ReceiveBufferSize { get; init; } = 16 * 1024;

    public long MaxMessageBytes { get; init; } = 1024 * 1024;

    public TimeSpan CloseTimeout { get; init; } = TimeSpan.FromSeconds(5);

    public EventDefinition<WebSocketConnectedPayload> ConnectedEvent { get; init; }
        = WebSocketEvents.Connected;

    public EventDefinition<WebSocketDisconnectedPayload> DisconnectedEvent { get; init; }
        = WebSocketEvents.Disconnected;

    public EventDefinition<WebSocketErrorPayload> ErrorEvent { get; init; }
        = WebSocketEvents.Error;

    public EventDefinition<WebSocketClosedPayload> ClosedEvent { get; init; }
        = WebSocketEvents.Closed;
}

public static class WebSocketEvents
{
    public static EventDefinition<WebSocketConnectedPayload> Connected { get; }
        = new("eventa:websockets:connected");

    public static EventDefinition<WebSocketDisconnectedPayload> Disconnected { get; }
        = new("eventa:websockets:disconnected");

    public static EventDefinition<WebSocketErrorPayload> Error { get; }
        = new("eventa:websockets:error");

    public static EventDefinition<WebSocketClosedPayload> Closed { get; }
        = new("eventa:websockets:closed");
}

public sealed record WebSocketConnectedPayload(string? SubProtocol);

public sealed record WebSocketDisconnectedPayload(
    WebSocketCloseStatus? CloseStatus,
    string? CloseStatusDescription);

public sealed record WebSocketErrorPayload(Exception Error);

public sealed record WebSocketClosedPayload(
    Exception? Error,
    WebSocketCloseStatus? CloseStatus,
    string? CloseStatusDescription);

public sealed class RemoteEventaException : Exception
{
    public string? RemoteName { get; }

    public string? RemoteStack { get; }
}

public sealed class WebSocketClosedException : Exception
{
    public WebSocketCloseStatus? CloseStatus { get; }

    public string? CloseStatusDescription { get; }
}
```

`WebSocketClientEndpoint` and `WebSocketPeerEndpoint` are directly usable as
`IEventContext`. They own an internal `EventContext`, a WebSocket adapter, a
receive pump, a send pump, and terminal lifecycle state. The per-peer server
endpoint intentionally accepts a raw `WebSocket` so the adapter package does not
need to own ASP.NET Core routing, middleware, authentication, or handshake
policy.

## Event Catalog

`WebSocketEventCatalog` is the explicit schema contract for one endpoint.

### Must

- Register every ordinary event id that can be received from a remote peer.
- Register every invoke definition that can cross the WebSocket boundary.
- Store the `JsonTypeInfo<T>` required to serialize and deserialize user
  request, response, and ordinary event payloads.
- Register all derived invoke event ids from one
  `InvokeEventDefinition<TResponse, TRequest>`.
- Treat event ids as ordinal, case-sensitive protocol identifiers.
- Reject duplicate registrations that bind the same wire event id to a different
  payload or invoke contract.
- Fail inbound messages with unregistered event ids before dispatching to
  `IEventInboundDispatcher.Receive`.

### Should

- Return `this` from registration methods to support compact setup code.
- Keep registration immutable after endpoint construction by snapshotting the
  catalog into endpoint-owned lookup tables.
- Keep diagnostics focused on the event id and expected registration.

### Audit Notes

The catalog is not optional. Inferring payload types from local listeners would
make inbound behavior depend on registration order, would fail for messages that
arrive before a listener is attached, and would encourage reflection-heavy code
that does not match the repository's AOT stance.

## Wire Protocol

The v1 wire format is the current TypeScript WebSocket adapter JSON wrapper:

```json
{
  "id": "message-id",
  "type": "event-id",
  "payload": {
    "id": "event-id",
    "type": "event",
    "body": {}
  },
  "timestamp": 1780215000000
}
```

The outer `type` is the concrete Eventa event id used for routing. The inner
`payload.id` must match the same concrete event id. The inner `payload.type`
uses the TypeScript Eventa event kind string `"event"`. The inner
`payload.body` carries the event body.

### Must

- Send text WebSocket messages only.
- Emit outer `id` as a new Eventa-style 16-character message id.
- Emit `timestamp` as Unix epoch milliseconds, matching TypeScript `Date.now()`.
- Serialize property names in camelCase for protocol payloads.
- Accept fragmented text frames by buffering until `EndOfMessage`.
- Reject binary frames in v1.
- Reject messages larger than `WebSocketEndpointOptions.MaxMessageBytes`.
- Reject malformed JSON, missing required wrapper fields, or mismatched
  `type` / `payload.id`.
- Dispatch valid remote messages through `IEventInboundDispatcher.Receive`.
- Never dispatch remote input through user `Emit`, because that would call
  `IEventaAdapter.OnSent` and create an echo loop.

### Should

- Ignore unknown optional wrapper fields for forward compatibility.
- Preserve the raw remote message id and timestamp in adapter metadata when
  practical, but not as part of the v1 public API contract.

### Audit Notes

The TypeScript adapter currently sends an inner `_flowDirection` field when it
serializes outbound events. C# may include `_flowDirection: "outbound"` for
compatibility, but C# dispatch must not depend on the field.

## Invoke Protocol Bridge

The adapter must bridge current C# invoke semantics to current TypeScript wire
semantics without rewriting the core invoke engine.

Current C# behavior:

- request events use the base `{tag}-send` event id
- response events use base `{tag}-receive`, `{tag}-receive-error`, and
  `{tag}-receive-stream-end` event ids
- invoke correlation lives in the body `InvokeId`

Current TypeScript WebSocket-compatible behavior:

- request events use the base `{tag}-send` event id
- request-stream chunks include `isReqStream: true`
- response events use per-invoke event ids:
  - `{tag}-receive-{invokeId}`
  - `{tag}-receive-error-{invokeId}`
  - `{tag}-receive-stream-end-{invokeId}`

### Must

- Add an optional `IsReqStream` marker to C# `SendPayload<TRequest>`.
- Set `IsReqStream=true` when C# request-stream clients emit request items.
- Serialize `SendPayload<TRequest>` as:

```json
{
  "invokeId": "invoke-id",
  "content": {},
  "isReqStream": true
}
```

- Omit `isReqStream` or write `false` for unary request payloads.
- When sending C# response, response-error, or response-stream-end events over
  WebSocket, rewrite the wire event id to the TypeScript per-invoke form.
- When receiving TypeScript per-invoke response event ids, map them back to the
  registered C# base response event ids before local inbound dispatch.
- Validate that the invoke id suffix in a per-invoke response event id matches
  the `body.invokeId` value.
- Keep C# local invoke clients and handlers subscribed to the existing base
  response event ids.

### Audit Notes

The bridge belongs in the WebSocket catalog/codec layer. A core parity rewrite
would touch the already-tested C# invoke session engine and would make this RFC
larger than a WebSocket adapter RFC. Deferring invoke compatibility would also
undercut the main reason to make the v1 wire format TypeScript-compatible.

## Remote Error Protocol

JSON cannot preserve arbitrary CLR or JavaScript exception objects. v1 therefore
uses a stable remote error DTO on the wire and maps it to a local exception.

Recommended wire shape:

```json
{
  "name": "Error",
  "message": "handler failed",
  "stack": "optional remote stack"
}
```

### Must

- Convert outbound `ReceiveErrorPayload.Error` values to the remote error DTO.
- Convert inbound remote error DTO values to `RemoteEventaException`.
- Preserve the remote message in `Exception.Message`.
- Preserve remote name and stack through `RemoteEventaException.RemoteName` and
  `RemoteEventaException.RemoteStack`.
- Never attempt to deserialize a remote exception as an arbitrary local
  exception type.

### Should

- Use the CLR exception type name as `name` for C#-originated errors.
- Include stack only when present; v1 does not need an option to suppress it.

### Audit Notes

User-defined error codecs are intentionally deferred. The first WebSocket
adapter needs a stable cross-language failure contract more than it needs
application-specific exception reconstruction.

## Internal Architecture

Each endpoint owns:

- one supplied WebSocket
- one snapshot of `WebSocketEventCatalog`
- one owned `EventContext`
- one `IEventInboundDispatcher` reference for remote input
- one `IEventTransportFatalNotifier` reference for transport death
- one private WebSocket adapter
- one outbound message queue
- one send pump that serializes `WebSocket.SendAsync` calls
- one receive pump that reconstructs complete text messages
- one terminal state and cancellation source

Outbound flow:

```text
local Emit
  -> local listener dispatch
  -> WebSocketAdapter.OnSent
  -> catalog encodes envelope to TS-compatible wrapper JSON
  -> outbound queue
  -> send pump
  -> WebSocket.SendAsync(text)
```

Inbound flow:

```text
WebSocket.ReceiveAsync(text frames)
  -> complete JSON message
  -> parse TS-compatible wrapper
  -> catalog resolves event id and JsonTypeInfo
  -> construct EventEnvelope<TPayload>
  -> inboundDispatcher.Receive(envelope, options)
  -> local listener dispatch + OnReceived only
```

`IEventaAdapter.OnSent` is synchronous. WebSocket sends are asynchronous.
Therefore `OnSent` must enqueue the encoded message and let a single send pump
own `SendAsync`. It must fail fast after terminal transition or when the
outbound queue cannot accept a message. The v1 default queue may be unbounded,
matching the channel adapter's "no high-throughput backpressure guarantee"
stance.

## Transport Failure Semantics

WebSocket close, WebSocket receive failure, WebSocket send failure, malformed
input, unknown event id, JSON deserialization failure, and local endpoint
disposal are transport terminal conditions.

### Must

When an endpoint observes a transport terminal condition, it must:

- enter terminal state once
- stop accepting new outbound sends
- complete the outbound queue
- cancel the receive and send pumps
- notify invoke session infrastructure through
  `IEventTransportFatalNotifier.NotifyTransportFatal`
- fault pending unary invoke sessions before public closed-event listeners run
- fault active stream invoke sessions before public closed-event listeners run
- best-effort dispatch the configured closed event after fatal notification
- dispose the owned context after closed-event dispatch is attempted
- complete `Completion`

### Should

- Normal remote close maps to `WebSocketClosedException`.
- WebSocket protocol errors preserve the original exception as the terminal
  cause.
- Local disposal maps to `WebSocketClosedException` with a local-disposal
  message.
- If possible, `DisposeAsync` should send a normal close frame before
  terminating locally, bounded by `WebSocketEndpointOptions.CloseTimeout`.

### Audit Notes

The deterministic transport fatal path is not normal event dispatch. It must not
depend on public closed-event listener order. It also must not emit invoke
`SendAbort` protocol events, because a transport failure is not a user
canceling one invoke.

## Lifecycle

### Must

`WebSocketClientEndpoint`:

- accepts a connected `ClientWebSocket` through its constructor
- exposes `ConnectAsync` as a convenience helper that creates, connects, and
  owns a `ClientWebSocket`
- starts receive and send pumps when the endpoint is created
- is directly usable as `IEventContext`
- implements `IDisposable` and `IAsyncDisposable`
- exposes `Completion` for server handlers and tests to wait for terminal state

`WebSocketPeerEndpoint`:

- accepts an already-accepted server-side `WebSocket`
- does not own ASP.NET Core routing or handshake policy
- starts receive and send pumps when the endpoint is created
- is directly usable as `IEventContext`
- implements `IDisposable` and `IAsyncDisposable`
- exposes `Completion`

Both endpoint types:

- reject user-initiated `Emit`, listener registration, unsubscription, and
  handler registration after terminal transition with `WebSocketClosedException`
- allow reusable invoke client factories to be created after terminal
  transition, but the first invoke use must fail fast
- dispatch the configured `ClosedEvent` at most once
- treat terminal cause as first-wins
- complete `Completion` exactly once

### Should

- `Connected`, `Disconnected`, and `Error` lifecycle events are best-effort
  business events. They are not the deterministic fatal path.
- `Closed` is the primary lifecycle event that tests and users can rely on for
  terminal observation.

### Audit Notes

Server peer endpoints are intentionally one context per accepted WebSocket. A
future broadcast or hub API can compose many peer endpoints, but v1 should not
define fan-out ordering, partial failure, or peer identity policy.

## AOT Compatibility

`Eventa.Adapters.WebSockets` must set `IsAotCompatible=true` and build cleanly
under trim and Native AOT analyzers.

The adapter must avoid:

- `dynamic`
- `Delegate.DynamicInvoke`
- `MethodInfo.Invoke`
- `MakeGenericMethod`
- `Activator.CreateInstance`
- reflection-based System.Text.Json serialization
- IL warning suppressions such as `#pragma warning disable IL*` or
  `UnconditionalSuppressMessage`

`JsonTypeInfo<T>` supplied to `WebSocketEventCatalog` is the only supported v1
payload serialization path. Built-in protocol DTOs can use source-generated or
manually encoded System.Text.Json metadata owned by the adapter package.

## Backpressure

The v1 adapter may use an unbounded outbound queue by default. This preserves the
current synchronous Eventa `Emit` shape and keeps v1 focused on correctness,
interoperability, and lifecycle semantics.

Bounded queues, explicit send pressure, drop policies, batching, and throughput
tuning are deferred. If a custom bounded queue is added later, `OnSent` must
still remain synchronous and fail fast rather than blocking indefinitely.

## Error Handling

### Must

Outbound send failure:

- If the outbound queue or WebSocket send pump is terminal, the local `Emit`
  path must surface a send failure.
- Invoke clients must surface local send failures through the existing invoke
  session send-fault behavior.

Inbound malformed input:

- Malformed JSON, unsupported binary frames, oversized messages, unknown event
  ids, missing payload fields, payload deserialization errors, and invoke bridge
  validation failures fault the endpoint.
- The endpoint then runs deterministic transport fatal notification and
  best-effort closed-event dispatch.

Inbound listener exception:

- If a user listener throws during remote inbound dispatch, the exception faults
  the endpoint and stops the receive pump.
- Pending unary invokes and active stream invokes must already be faulted before
  the public closed event runs.

### Audit Notes

Disposal is not malformed input. Disposal uses the normal endpoint terminal
path and should preserve first terminal cause when it races with receive or send
failure.

## Verification Criteria

- Transport smoke: ordinary events, unary invoke, request-stream unary invoke,
  server-streaming invoke, and bidirectional streaming invoke work between a C#
  client endpoint and a C# server peer endpoint.
- TypeScript wrapper compatibility: generated JSON contains `id`, `type`,
  `payload`, and `timestamp`; protocol payloads use camelCase names.
- TypeScript invoke compatibility: C# request-stream sends include
  `isReqStream`; outgoing responses use per-invoke response event ids; incoming
  per-invoke response ids map back to C# base response event ids.
- Catalog behavior: registered ordinary events and invoke events deserialize
  correctly; unknown event ids and duplicate incompatible registrations fail
  clearly.
- Remote errors: C# handler exceptions become remote error DTOs on the wire;
  inbound remote errors surface as `RemoteEventaException`.
- Fatal path: socket close, socket error, malformed JSON, unknown event id,
  send-pump failure, and inbound listener exceptions fault pending unary invokes
  and active stream async enumerables before public closed-event listeners run.
- Lifecycle: `Dispose`, `DisposeAsync`, remote close, concurrent terminal
  races, and `Completion` are idempotent and first-cause-wins.
- AOT: the new package sets `IsAotCompatible=true` and builds without trim or
  Native AOT analyzer warnings.
- Documentation: README or package docs show client usage, ASP.NET Core
  per-peer usage, catalog registration, TypeScript-compatible scope, and v1
  non-goals.

## Documentation

Add a README section after implementation. The client example should show a
source-generated JSON context and explicit catalog registration:

```csharp
var catalog = new WebSocketEventCatalog()
    .RegisterInvoke(echo, AppJsonContext.Default.EchoResponse, AppJsonContext.Default.EchoRequest);

await using var endpoint = await WebSocketClientEndpoint.ConnectAsync(uri, catalog);
var echoClient = endpoint.CreateInvokeClient(echo);
var response = await echoClient.InvokeAsync(new EchoRequest("eventa"));
```

The server example should show ASP.NET Core accepting one WebSocket and creating
one `WebSocketPeerEndpoint` per connection. Documentation must state that v1 is
not a broadcast hub and that users compose multiple peer endpoints themselves.

Document that the adapter is TypeScript-wire-compatible with the current
`@moeru/eventa/adapters/websocket` wrapper, but that user payload compatibility
still depends on both sides agreeing on JSON payload shapes.

## References

- TypeScript WebSocket wrapper:
  `src/adapters/websocket/shared.ts`,
  `src/adapters/websocket/internal.ts`
- TypeScript native WebSocket adapter:
  `src/adapters/websocket/native/index.ts`
- TypeScript H3 per-peer adapter:
  `src/adapters/websocket/h3/peer.ts`
- TypeScript global broadcast adapter:
  `src/adapters/websocket/h3/global.ts`
- TypeScript invoke wire behavior:
  `src/invoke.ts`, `src/stream.ts`, `src/invoke-shared.ts`
- C# channel adapter RFC:
  `playground/docs/rfcs/rfc-1-channel-adapter.md`
- C# invoke protocol bindings:
  `playground/src/Eventa/Support/InvokeEventBindings.cs`
- System.Text.Json source generation:
  <https://learn.microsoft.com/en-us/dotnet/standard/serialization/system-text-json/source-generation>
- ASP.NET Core WebSockets:
  <https://learn.microsoft.com/en-us/aspnet/core/fundamentals/websockets>

## Alternatives Considered

### Put WebSocket Types In An Aggregate `Eventa.Adapters` Package

Rejected. There is no aggregate `Eventa.Adapters` package; the existing channel
adapter is its own `Eventa.Adapters.Channels` package and has no network or JSON
dependency surface. A separate `Eventa.Adapters.WebSockets` package avoids
making channel users accept WebSocket-specific dependencies and future protocol
churn.

### Client Only

Rejected for v1. A client-only package would not prove the full Eventa invoke
and stream matrix in C# and would make TypeScript compatibility harder to
validate. The per-peer server endpoint is the smallest server model that covers
real bidirectional RPC without defining broadcast or hub policy.

### Full TypeScript Global Context Parity

Rejected for v1. TypeScript has a global H3 context that broadcasts outbound
events to all peers. C# should defer this because broadcast requires explicit
policy for peer identity, partial failure, ordering, backpressure, and
per-peer lifecycle.

### Infer Payload Types From Local Subscriptions

Rejected. It makes message handling order-sensitive, fails before subscriptions
exist, and does not fit AOT. The catalog is explicit by design.

### Core Invoke Protocol Rewrite

Rejected for this RFC. Mapping TypeScript per-invoke response ids in the adapter
preserves the tested C# core invoke engine and keeps the WebSocket package
responsible for WebSocket compatibility.

### User-Supplied Error Codec

Deferred. A stable remote error DTO gives v1 predictable cross-language
behavior. Application-specific error reconstruction can be added later without
changing the basic DTO.

## Open Questions

None.
