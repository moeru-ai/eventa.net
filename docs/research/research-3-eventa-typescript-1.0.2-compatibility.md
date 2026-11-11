# Eventa TypeScript 1.0.2 and .NET Core Compatibility

Updated: 2026-11-11

## Overview

Eventa.NET provides an idiomatic .NET event API with TypeScript-compatible protocol semantics. Its foundation is a transport-neutral routing envelope. JSON, structured serialization, and in-process values belong to individual Adapters and profiles; they are not a universal Core byte protocol.

The new Delivery model requires `contentType`, which vanilla TypeScript 1.0.2 frames do not contain. Wire interoperability with those Adapters therefore requires an explicit compatibility profile; see [§4.7](#47-compatibility-layers).

This document proceeds from TypeScript behavior to current .NET gaps, .NET contracts, implementation scope, open choices, and acceptance tests. Section 2 records TypeScript 1.0.2 source facts only. Section 3 describes the existing .NET implementation. Section 4 defines the Core and Invoke implementation baseline. Section 6 contains only implementation choices and interfaces still to be defined.

Implementation is Core-first: Delivery and Core, then Invoke, then typed helpers and concrete Adapters. Core contracts and Adapter architecture contracts are settled; concrete Adapter implementation is deferred. Application orchestration belongs to user code and does not require an Eventa-owned central runtime.

## 1. Scope, compatibility goals, and sources

### 1.1 Document responsibilities

This document defines the Core envelope, routing, recent-delivery duplicate suppression, listener dispatch and completion, hop semantics, Invoke protocol, transparent relay, and Adapter architecture contracts.

[Research 4](research-4-eventa-body-codec-boundary.md) covers detailed body ownership, typed codecs, System.Text.Json, AOT metadata, JSON value domains, remote errors, and missing/null handling. This document includes only the body and typed boundaries needed to understand Core. The P1 candidate JSON profile is not part of the base protocol.

Compatibility has three layers: Core semantics, Adapter wire format, and typed body contracts; see [§4.7](#47-compatibility-layers). Breaking changes are allowed. The refactor does not provide backward compatibility or negotiation for the old C# transport contract.

### 1.2 TypeScript version and primary sources

The baseline is the `@moeru/eventa` `v1.0.2` tag, whose package version is `1.0.2`. [package.json](https://github.com/moeru-ai/eventa/blob/v1.0.2/package.json#L1-L5)

The primary source files are:

- `src/eventa.ts`: Eventa event objects.
- `src/internal.ts`: transport-neutral `EventaInner`.
- `src/context.ts`: routing, dispatch, duplicate suppression, and hop budgets.
- `src/channel.ts`: forwarding between Contexts and opt-in transforms.
- `src/invoke-shared.ts`, `src/invoke.ts`, and `src/stream.ts`: Invoke control protocol.
- `src/adapters/*`: carrier encoding and lifecycle behavior.

Section 2 describes that tag. Eventa.NET target contracts are defined separately in §4.

### 1.3 .NET source baseline

The package project is [Eventa.csproj](../../src/Eventa.csproj). Core code is under `src/Eventa`; the Channels Adapter is under `src/Eventa.Adapters/Channels`. Section 3 analyzes this existing implementation, separately from the target contracts in §4.

## 2. TypeScript 1.0.2 source facts

This section records TypeScript source structures and runtime behavior only. Current C# gaps are in [§3](#3-current-eventanet-gaps); target contracts are in [§4](#4-eventanet-contracts).

### 2.1 Core routes runtime objects, not JSON frames

`EventaInner<T>` is a transport-neutral runtime envelope with three common fields. [`EventaInner`](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/internal.ts#L24-L31)

#### `deliveryId`

A stable delivery identity for one emission across fan-out, Contexts, and transports.

#### `hopsRemaining`

The remaining edge-crossing budget. A zero-hop delivery can still dispatch locally but cannot be forwarded further.

#### `eventa`

The application event object. It has at least a string `id`; `type`, `body`, `metadata`, and `invokeMetadata` are optional. [`EventaLike` and `Eventa`](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/eventa.ts#L52-L80)

#### Routing gate and envelope validation

`accept` is the internal routing and dispatch gate inside `createContext()`, not a public `EventContext` member. It first deduplicates by `deliveryId`. For a positive hop budget, it uses `setEventaInner` to associate continuation state with `hopsRemaining - 1`, then dispatches the same `eventa` locally. At zero hops it still dispatches locally and reports a `hops-exhausted` diagnostic. This decrement is continuation bookkeeping; it does not parse the business body. [internal `accept`](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/context.ts#L146-L169)

The optional `routing.onDiagnostic` callback reports that diagnostic. A synchronous callback exception is captured as a rejected Promise; local dispatch still runs. The rejection joins the internal `accept` operation's `Promise.all` failure path and can make its completion fail. This is TypeScript 1.0.2's local callback behavior. [diagnostic callback and dispatch](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/context.ts#L151-L168)

The public Context exposes `emit/on/once/off/signal/abort`, not `accept`. `emit` creates a new Eventa object with `{ ...event, body: payload }`, restores or creates its inner envelope, and calls the internal gate. Application and Adapter code cannot call that gate through the public Context API. References below to dispatching the same `eventa` mean the object processed by one `accept` and passed to that dispatch's listeners, not preservation of the caller's original Eventa reference. [`emit`](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/context.ts#L171-L178) [public context surface](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/context.ts#L181-L307)

`isEventaInner` checks only the outer object, non-empty `deliveryId`, non-negative safe-integer `hopsRemaining`, object `eventa`, and string `eventa.id`. It does not validate the body, metadata, or additional fields. The common Core contract is therefore a minimal routable envelope, not a closed JSON schema. [`isEventaInner`](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/internal.ts#L62-L76)

#### Recent-delivery history and dispatch completion

`hasSeen` synchronously checks delivery IDs before dispatch and immediately records unseen IDs in `recentDeliveries`. A duplicate causes internal `accept` to return an already-successful Promise without dispatching again or joining the first dispatch's Promise. Defaults are a five-minute `recentDeliveryTtl` and a `recentDeliveryLimit` of 10,000. IDs can dispatch again after expiration or capacity eviction. [routing defaults](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/context.ts#L7-L32) [`hasSeen` and `accept`](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/context.ts#L74-L109) [duplicate return](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/context.ts#L146-L169)

Listener failure does not remove recent-history entries. Even a delivery with no matching listener is recorded and completes successfully. `dispatch` invokes matching listeners individually, captures synchronous throws as rejected Promises, continues invoking subsequent listeners, and collects returned Promises. [listener invocation and aggregation](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/context.ts#L111-L144)

`Promise.all(pending)` waits for every result on success. Once a pending Promise rejects, the aggregate rejects without waiting for other already-started listeners to finish. It does not automatically cancel them. This completion behavior does not allow one failing listener to prevent subsequent callbacks from being invoked.

### 2.2 Opaque routing bodies and module-specific structures

An ordinary event's `body?: P` is a generic runtime value. Context dispatch selects by `eventa.id`; all matching listeners in one dispatch receive the same Eventa object, newly created by `emit`. Core has no global payload registry, JSON converter, or wire type system.

Channel plugins can inspect, replace, or discard an event. This is an opt-in transform boundary, not a requirement for basic routing to interpret bodies. [`ChannelPlugin`](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/channel.ts#L8-L34) [`applyPlugins`](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/channel.ts#L248-L279)

Invoke and stream are protocol modules built on ordinary events. They interpret their own control fields. The basic router does not interpret application bodies, and request/response `content` remains a generic value.

### 2.3 Adapters determine carriers and value domains

#### JSON text adapters

Native WebSocket serializes the complete inner envelope with `JSON.stringify`; inbound data is parsed with `JSON.parse` before `restoreInner`. [Native WebSocket adapter](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/adapters/websocket/native/index.ts#L24-L42)

H3 and Hono WebSocket use the same approach. Unix sockets use newline-delimited JSON and enforce a frame byte limit inside the Adapter. [Unix socket adapter](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/adapters/unix-socket/index.ts#L61-L100)

The resulting body restrictions come from the JSON implementation used by the Adapter, not from Eventa Core.

#### Structured serialization and host IPC adapters

BroadcastChannel passes the inner envelope to `postMessage`, whose structured serialization determines supported values; this Adapter supplies no transfer list. The browser Worker Adapter first shallow-copies Eventa/body and normalizes the body and transfer list from `WorkerEventa` or an extendable Invoke response. It then passes the inner envelope and optional transfer list to `postMessage`. Transfer ownership therefore applies to Worker paths that actually supply a transfer list, not to BroadcastChannel in general. [BroadcastChannel adapter](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/adapters/broadcast-channel/index.ts#L51-L70) [Worker adapter](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/adapters/webworkers/index.ts#L24-L48) [Worker body/transfer normalization](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/adapters/webworkers/internal.ts#L11-L25) [Worker-specific normalization](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/adapters/webworkers/shared.ts#L42-L68)

Window messaging, Node Worker Threads, and Electron IPC also pass runtime values to their host APIs. Node Worker Threads can transfer ownership through a transfer list. Worker Adapters move transfer information from certain event/response bodies into out-of-band host arguments. These are not JSON Core protocol operations. [Node Worker Threads adapter](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/adapters/worker-threads/index.ts#L30-L42)

#### In-process adapters

EventTarget and EventEmitter pass runtime objects directly without cross-language serialization. [EventEmitter adapter](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/adapters/event-emitter/index.ts#L37-L57)

#### Consequence

TypeScript 1.0.2 has no single body wire encoding shared by every Adapter. JSON, structured-clone, and in-process Adapters can support different values and have different failure modes for the same business body.

### 2.4 JSON adapters still materialize bodies

An opaque-body boundary defines semantic responsibility; it does not imply streaming, header-only parsing. TypeScript WebSocket and Unix implementations call `JSON.parse` and materialize the entire JSON tree before Core treats its body as opaque.

### 2.5 Adapter compatibility frames

Adapters send a combined object produced by `createOutboundInner`, which calls `createAdapterInner` to add beta.13 aliases. The following example illustrates field relationships, not a literal wire capture. Real outbound events carry `_flowDirection: "outbound"`, and `timestamp` comes from `Date.now()`; the value `0` below is a placeholder.

```json
{
  "deliveryId": "delivery-id",
  "hopsRemaining": 31,
  "eventa": {
    "id": "orders:created",
    "type": "event",
    "_flowDirection": "outbound",
    "body": {}
  },
  "id": "delivery-id",
  "type": "orders:created",
  "payload": {
    "id": "orders:created",
    "type": "event",
    "_flowDirection": "outbound",
    "body": {}
  },
  "timestamp": 0
}
```

`deliveryId/hopsRemaining/eventa` form the current `EventaInner`. `id` aliases `deliveryId`; `type` and `payload` correspond to `eventa.id` and `eventa`; `timestamp` is an additional beta.13 field. [`createOutboundInner` and `createAdapterInner`](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/adapters/internal.ts#L31-L58)

`restoreInner` prefers the current inner format and reads the legacy-only shape only if current validation fails. [`restoreInner`](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/adapters/internal.ts#L60-L93)

### 2.6 Invoke control bodies and classification markers

TypeScript 1.0.2's `EventaInner` has no `contentType`. `defineInvokeEventa` creates seven event IDs and attaches a numeric `invokeType` marker to each definition. Control fields are carried in `eventa.body`. [`defineInvokeEventa`](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/invoke-shared.ts#L98-L140)

#### Send

The ID is `${tag}-send`. Control fields are `invokeId`, `content`, and optional `isReqStream`. Every request-stream chunk must have `isReqStream: true`.

#### Send error

The ID is `${tag}-send-error`. The body contains `invokeId` and error `content`. It terminates the request stream.

#### Send stream end

The ID is `${tag}-send-stream-end`. The body contains at least `invokeId`. Runtime `content` is `undefined`, so JSON Adapters omit that property.

#### Send abort

The ID is `${tag}-send-abort`. The body contains `invokeId` and optional reason `content`.

#### Receive

The base ID is `${tag}-receive`; the actual response ID is `${base}-${invokeId}`. The body contains `invokeId` and response `content`.

#### Receive error

The base ID is `${tag}-receive-error`; the actual ID also appends the invocation ID. The body contains `invokeId` and `content: { error }`.

#### Receive stream end

The base ID is `${tag}-receive-stream-end`; the actual ID appends the invocation ID. The body contains at least `invokeId`.

These shapes are defined in `invoke-shared.ts` and consumed by `invoke.ts` and `stream.ts`. [`invoke-shared.ts`](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/invoke-shared.ts#L15-L48) [`invoke.ts`](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/invoke.ts#L251-L273) [`stream.ts`](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/stream.ts#L109-L145)

#### `InvokeEventType`

`invokeType` is a runtime classification marker on TypeScript event definitions, also used by type guards and the Tauri Adapter. It is enumerable, so a JSON Adapter serializing the whole event may include it. Registered Invoke listeners still correlate requests and responses through event IDs and control fields. [`isInvokeEventa`](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/invoke-shared.ts#L143-L186) [Tauri adapter](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/adapters/tauri/index.ts#L40-L57)

### 2.7 Invoke registration, ordering, and lifecycle

#### Ordering

Independent emits have no global ordering guarantee. Request-stream chunks precede their own end/error terminal; each responder preserves the same ordering within its own response stream. There is no stable global order between responders. Cancellation is an independent routed event and may overtake in-flight data. [README routing semantics](https://github.com/moeru-ai/eventa/blob/v1.0.2/README.md#L121-L123) [response stream pump](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/stream.ts#L300-L337)

A request stream need not contain any data chunks. For one `invokeId`, stream-end or send-error can be its first request-stream frame. That terminal uses normal invocation correlation; a zero-chunk or terminal-first request stream is valid, not malformed.

#### Handler registration and disposer lifecycle

`defineInvokeHandler` groups registrations in `invokeHandlers` by request/send event ID, then by handler function identity. Distinct handlers can register for the same event identity; registering the same function again reuses its registration. Context dispatch invokes every matching listener, allowing distinct handlers to process the same request. [handler registry](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/invoke.ts#L456-L488) [Context dispatch](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/context.ts#L111-L144)

Tests verify that duplicate registration invokes one function once, that one Invoke calls two distinct handlers, and that removing one registration leaves the other intact. [registration tests](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/invoke.test.ts#L129-L164)

A removed handler function can be registered again. Tests cover a single-handler case in which a stale disposer from a duplicate registration runs after removal and re-registration, while the new listener still handles the invocation. This does not establish generation isolation for every registry state: `removeHandler` unconditionally calls `handlers.delete(handler)` on its captured map. If a sibling handler keeps that map alive across removal and re-registration, an old disposer may delete the new registry entry while its protocol listener remains in Context. Existing tests do not cover that sibling-handler case. [re-registration and stale-disposer tests](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/invoke.test.ts#L166-L197) [handler registry removal](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/invoke.ts#L468-L483)

A registration disposer removes protocol listeners and registry ownership without actively aborting started invocations. `InvokeState.dispose()` only removes its Context-abort listener. An active handler is not forcibly stopped, but its request stream may lose subsequent chunk/end/error frames; its disposed state also stops receiving later Context-abort signals. Retaining local handler state therefore does not guarantee continued input or eventual settlement. Registration removal and invocation cancellation are distinct operations. [handler disposer](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/invoke.ts#L440-L480) [InvokeState.dispose](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/invoke-state.ts#L147-L149)

#### Response correlation and caller completion

Consumers check both the dynamic response event ID and body `invokeId`. A mismatch is silently ignored.

Responders to one invocation use the same dynamic response event identity and `invokeId`, but each response emission has an independent `deliveryId`; these emissions are not duplicate forwarding of one logical Delivery. Stream handlers likewise derive response IDs from the invocation ID and emit chunks and terminals separately. [unary response emission](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/invoke.ts#L491-L519) [stream response emission](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/stream.ts#L295-L337) [emit and inner creation](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/context.ts#L171-L178) [new delivery identity](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/internal.ts#L33-L39)

A unary caller does not select a responder owner. Its first response or error matching the dynamic event ID and body `invokeId` sets `finished` and removes invocation listeners. Later results cannot change the settled Promise. An error may arrive first and fail the call; this is not first-success-wins. [unary caller](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/invoke.ts#L220-L273)

A streaming caller enqueues correlated chunks while not stopped, without binding to the first responder. The first valid correlated stream-end or error terminal sets `stopped`, closes or fails the stream, and removes listeners. Later frames cannot affect the caller. Normal response/terminal completion sends no abort and does not implicitly cancel other responders; explicit cancellation and Context lifetime use separate paths. [stream caller](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/stream.ts#L76-L169)

#### Recommended topology

The README recommends one effective Invoke handler in its server topology example. This is an Application topology recommendation, not a protocol or registration invariant. The implementation does not enforce a single responder. [README server topology](https://github.com/moeru-ai/eventa/blob/v1.0.2/README.md#L125-L137)

#### Early abort

Abort may arrive before a request. `InvokeState` retains a cancellation tombstone for an unknown invocation ID. The first later request materializes one cancelled handler invocation; subsequent frames with that ID are suppressed. Once `abortReasons` exceeds 10,000 entries, it evicts the oldest records in insertion order. [`InvokeState`](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/invoke-state.ts#L10-L117)

These are facts about the `v1.0.2` tag, not current `main`. The later commit `fbd964f` refactored Invoke cancellation state. Questions about a current branch require checking its current `src/invoke-state.ts`.

TypeScript 1.0.2 has no Invoke tracker-specific diagnostic surface. Tombstone retention, capacity eviction, ignored frames, and invocation-state cleanup are internal `InvokeState` bookkeeping, with no tracker diagnostic callback, event, or eviction notification. This is separate from Context's `routing.onDiagnostic`. [InvokeState bookkeeping](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/invoke-state.ts#L10-L149)

#### Context death

Context exposes a local `AbortSignal`; transport Adapters abort it on closure or failure. The Invoke client listens to that signal and ends pending invocations. Handler state aborts active handlers' `AbortController`s and errors their request streams. Cancellation is cooperative: handlers must observe the signal or stream error, and Eventa cannot forcibly terminate user code that ignores cancellation. Context lifetime is not encoded as an abort frame for one remote invocation. [`InvokeState` context abort](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/invoke-state.ts#L33-L49) [`defineInvokeHandler` lifecycle](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/invoke.ts#L487-L519)

#### Late responses

After pending listeners are removed, late responses have no consumer.

### 2.8 Metadata and extension fields

`metadata` and `invokeMetadata` are optional event data carried through the relevant Adapters. Emit options and raw host events belong only to local Adapter call chains. Ordinary `metadata` is not retained on every Invoke path: `defineInvoke` deletes it from local response-listener definitions, and source comments allow handlers to omit it. This does not establish the same omission behavior for `invokeMetadata`. [Eventa metadata fields](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/eventa.ts#L57-L79) [`defineInvoke` response listeners](https://github.com/moeru-ai/eventa/blob/v1.0.2/src/invoke.ts#L188-L197)

Worker transferable ownership, window-message wrappers, Electron/Tauri host information, and remote-method function stubs belong to specific Adapters or opt-in modules. They are not universal Adapter fields or behavior.

## 3. Current Eventa.NET gaps

This section describes gaps in the existing implementation. Target APIs and behavior are defined in §4.

### 3.1 Envelope, delivery identity, and relay boundary

The current API centers on local typed dispatch. `EventEnvelope<TPayload>` stores a CLR `TPayload`; `IEventEnvelope` exposes the erased payload through `Type` and `object`. `IEventaAdapter` and the Channels Adapter receive the same CLR envelope.

Typed dispatch and transport messages are not separated, making CLR type identity an implicit transport dependency. This works for in-process CLR/Channels usage, but cross-language Core requires a typed application view separate from the transport-neutral routing envelope. [EventEnvelope.cs](../../src/Eventa/Primitives/EventEnvelope.cs) [IEventEnvelope.cs](../../src/Eventa/Abstractions/IEventEnvelope.cs) [IEventaAdapter.cs](../../src/Eventa/Abstractions/IEventaAdapter.cs) [ChannelMessage](../../src/Eventa.Adapters/Channels/ChannelMessages.cs) [ChannelAdapter](../../src/Eventa.Adapters/Channels/ChannelAdapter.cs)

The envelope contains only event ID and typed body; Channels messages add local options. There is no delivery identity, hop budget, or recent-delivery duplicate suppression for multi-hop routing. There is also no boundary for handing a Delivery projection and retained carrier representation to Application, so representation-preserving transparent relay is not supported. [ChannelMessages.cs](../../src/Eventa.Adapters/Channels/ChannelMessages.cs) [ChannelEndpoint.cs](../../src/Eventa.Adapters/Channels/ChannelEndpoint.cs)

### 3.2 Invoke frames and correlation

`InvokeEventDefinition` declares seven suffixes, but runtime `InvokeEventBindings` has only six definitions and lacks send-error. The request-stream client repeatedly sends ordinary `SendPayload<T>` without `isReqStream: true`. Producer failure ends the local session without sending send-error. TypeScript interprets those chunks as separate unary requests, a protocol incompatibility that must be fixed. [EventDefinition.cs](../../src/Eventa/Primitives/EventDefinition.cs) [InvokeEventBindings.cs](../../src/Eventa/Support/InvokeEventBindings.cs) [InvokeClient.cs](../../src/Eventa/InvokeClient.cs)

C# responses currently use fixed base event IDs and filter by body `InvokeId`. TypeScript uses a dynamic ID ending in the invocation ID and checks the body ID again. [InvokeSessionEngine.cs](../../src/Eventa/Support/InvokeSessionEngine.cs) [InvokeHandlerRegistrationFactory.cs](../../src/Eventa/Support/InvokeHandlerRegistrationFactory.cs)

### 3.3 Error and abort representation

Bindings pass CLR objects such as `SendPayload<T>`, `ReceivePayload<T>`, and `Exception` directly to transport. `SendErrorPayload` and `ReceiveErrorPayload` carry CLR exceptions, while `AbortPayload` limits the reason to a nullable string. These local typed views do not define cross-language body representations. [InvokeEventBindings.cs](../../src/Eventa/Support/InvokeEventBindings.cs)

The target separates control and terminal semantics from business-content codecs; see §4.5. Research 4 defines JSON error/reason helpers.

### 3.4 Early abort

The unary tracker discards aborts for unknown invocation IDs, while the request-stream path may create state and start a handler for one. Neither path reliably handles abort overtaking the first request. [InvocationTrackers.cs](../../src/Eventa/Support/InvocationTrackers.cs) [InvokeHandlerRegistrationFactory.cs](../../src/Eventa/Support/InvokeHandlerRegistrationFactory.cs)

The unified early-abort contract and local resource policy are in [§4.5](#45-invoke-protocol-contract).

## 4. Eventa.NET contracts

This section defines the data model, responsibility boundaries, and observable behavior for Core and Invoke. Internal implementation choices and open interfaces are listed in §6.

### 4.1 Delivery and Eventa model

Delivery carries routing information. Nested Eventa contains only an event ID and opaque body. These types and their structural view establish the model used by Core ingress and forwarding.

#### CLR type and identity

`Delivery` is a non-generic, immutable, non-positional `sealed record class Delivery : IDeliveryLike`. It is a Core model, not a requirement that every Adapter serialize the CLR object directly. Each Adapter/profile owns framing and carrier representation. Keeping the basic nested TypeScript 1.0 shape reduces the cost of reading cross-language code, logs, and bridges.

`sealed` prevents subtypes from adding state that changes Core semantics. Record value equality and harmless shallow copies are CLR conveniences. Next-hop envelopes must use the controlled `Forward()` construction path. Protocol identity is always `deliveryId`; duplicate suppression must not use record equality, a whole-object hash, or reference equality.

#### Protocol shape and naming

Delivery retains the nested shape rather than flattening event identity and body into `EventId`/`Body`:

```text
Delivery
├── contentType
├── deliveryId
├── hopsRemaining
└── eventa
    ├── id
    └── body
```

Only protocol-facing members on Delivery, Eventa, and `IDeliveryLike` use camelCase: `contentType`, `deliveryId`, `hopsRemaining`, `eventa`, `id`, and `body`. Other CLR APIs, including `IDeliveryLike`, `EventDefinition<T>`, `SendAsync`, `CancellationToken`, and `Forward`, follow normal .NET naming.

The fixed Eventa model contains only string `id` and `object? body`. `type`, `metadata`, `invokeMetadata`, and unknown properties stay outside Core; preservation is defined in [§4.6](#46-retained-carrier-representation-and-transparent-relay).

#### Eventa CLR type

Eventa is an ordinary sealed class with this constructor and these properties:

```csharp
public sealed class Eventa
{
    public string id { get; }
    public object? body { get; }

    public Eventa(string id, object? body = null)
    {
        ArgumentNullException.ThrowIfNull(id);

        this.id = id;
        this.body = body;
    }
}
```

`id` rejects only null. Empty and whitespace-only strings are valid; do not use `ThrowIfNullOrWhiteSpace`. Omitting the body stores null.

Eventa has no record equality, copy/`with` semantics, generic extension dictionary, or `JsonExtensionData`. Delivery construction, forwarding, and fan-out retain the same Eventa and body references.

#### `IDeliveryLike` structural view

Core exposes this minimal structural view:

```csharp
public interface IDeliveryLike
{
    string? contentType { get; }
    string? deliveryId { get; }
    int hopsRemaining { get; }
    Eventa? eventa { get; }
}
```

`IDeliveryLike` means that a value has enough Delivery structure to attempt restoration; it does not mean an untrusted DTO. A decoded Adapter/profile carrier or compatibility-profile output can implement it directly. Core adds neither a `DeliveryLike` DTO class nor `IEventaLike`. Delivery implements the interface with non-null reference properties.

Nullable reference members allow implementations to represent input that does not yet satisfy Core invariants. `hopsRemaining` is already an `int` at this boundary; the Adapter/profile decoder rejects fractional wire numbers and values outside `Int32`.

The interface does not own serialization, extension preservation, Adapter lifecycle, routing, or validation policy. Transparent relay associates retained carrier representation with the Delivery projection in an Application/profile carrier outside Core.

#### Field types and minimum invariants

`deliveryId` is a string, not a new `DeliveryId` value type. It must not be null or empty. Like TypeScript 1.0.2's `.length > 0` check, this permits whitespace-only strings.

New deliveries use Nano ID. TypeScript's `nanoid` and Eventa.NET's existing `pblasucci.ananoid` are implementation dependencies; their types do not enter the protocol or public model. Restoration preserves the sender's ID.

`hopsRemaining` is a non-negative `int`. New deliveries default to 32, exposed as the public constant `Delivery.DefaultHopsRemaining`.

`int` is a natural .NET counter/budget type and exposes an erroneous decrement as `-1` rather than unsigned underflow. It intentionally narrows TypeScript's non-negative safe-integer domain. Routing does not need budgets near `2^53 - 1`; decoders reject wire values beyond `Int32`.

An inbound Adapter/profile decoder validates at least an outer object, non-empty string `deliveryId`, non-negative integral `hopsRemaining` representable by `int`, non-null object `eventa`, and string `eventa.id`. Fractional, negative, and greater-than-`Int32.MaxValue` hop values are rejected.

`eventa.id` need not be non-empty or non-whitespace. Even an empty ID meets TypeScript 1.0.2's minimum contract. `eventa.body` is `object?`; Core does not check its business type, value domain, or property presence.

`contentType` is a non-null string in Delivery and `string?` in `IDeliveryLike`. Core adds no Media Type value object or Eventa-specific wrapper.

`Create` and `Restore` share Media Type syntax validation and preserve the exact valid input string without trimming or canonicalization.

String storage does not permit string-based protocol detection. `protocol=invoke` requires semantic parameter lookup through a real Media Type parser, not substring matching, fixed parameter order, or full-string ordinal equality. Parser/library and internal helper choices are listed in §6.1.

#### Constructor and record copy safety

The type, constant, properties, and constructor have this shape; the public construction methods are implemented on this type:

```csharp
public sealed record class Delivery : IDeliveryLike
{
    public const int DefaultHopsRemaining = 32;

    public string contentType { get; }
    public string deliveryId { get; }
    public int hopsRemaining { get; }
    public Eventa eventa { get; }

    private Delivery(
        string contentType,
        string deliveryId,
        int hopsRemaining,
        Eventa eventa)
    {
        this.contentType = contentType;
        this.deliveryId = deliveryId;
        this.hopsRemaining = hopsRemaining;
        this.eventa = eventa;
    }
}
```

All protocol properties are get-only, with no public or internal `init` setters. The private constructor restricts both external callers and other assembly modules to the supported construction paths. Initializers and field-changing `with` expressions cannot bypass invariants.

An empty `delivery with { }` is a valid shallow copy. It needs no special copy-constructor machinery, changes no invariant, and creates no new delivery identity.

#### Construction API

The public construction API is:

```csharp
public static Delivery Create(string contentType, Eventa eventa)
public static Delivery Restore(IDeliveryLike delivery)
public Delivery Forward()
```

#### `Create`: a new logical delivery

`Create` validates a non-null Eventa and valid, non-null `contentType`, retains the same Eventa reference, generates a non-empty Nano ID with the existing dependency, and sets hops to `DefaultHopsRemaining` (32).

The signature accepts neither a caller-selected delivery ID nor a hop budget. The exact Nano ID package call is an implementation detail. Do not add `IDeliveryIdGenerator`, generator DI for tests, or public Nano ID implementation types.

#### `Restore`: an existing valid delivery

`Restore` is the `IDeliveryLike → Delivery` invariant boundary. A null argument throws `ArgumentNullException`. An existing Delivery is returned unchanged, with no allocation or field generation.

For another implementation, validate non-null, syntactically valid `contentType`; non-null `deliveryId` with `Length > 0`; `hopsRemaining >= 0`; and non-null Eventa. Whitespace-only delivery IDs are valid.

Invalid members are construction-argument invariant violations and throw `ArgumentException`. P0 introduces no `InvalidDeliveryException`; exception message wording remains an implementation detail.

Successful restoration preserves the exact content-type string, delivery-ID string, hop value, and Eventa/body references. It does not generate or trim IDs, reset or clamp hops, canonicalize content types, infer missing representations, fill values from `BinaryData.MediaType`, create or clone Eventa, or repair malformed input. The Eventa constructor already guarantees `id != null`; Restore need not validate it again.

Adapter/profile decoding and explicit compatibility profiles ultimately supply a complete `IDeliveryLike`. Restore does not interpret old TypeScript compatibility rules. A vanilla 1.0.2 frame without `contentType` requires an explicit profile mapping first.

#### `Forward()`: a next-hop envelope

`original.Forward()` returns a new Delivery, leaves the original unchanged, retains `deliveryId`, the exact `contentType` string, and the same Eventa/body references, and sets hops to `original.hopsRemaining - 1`.

At zero hops it throws `InvalidOperationException`, although local dispatch is still allowed. There is no static `Delivery.Forward(delivery)`, `TryForward`, or `CanForward` API.

Two calls on the same hop-10 original produce hop-9 branches. Only a chain such as `original.Forward().Forward()` reaches hop 8.

Forward only derives an envelope. Application decides whether to forward, selects the target and Adapter, and calls `SendAsync`/`TrySend`. Adapter admission and sending must not implicitly decrement hops.

### 4.2 Body representation, contentType, and ownership

Core introduces no `EventBody`, `OpaqueBody`, or body-kind hierarchy. `eventa.body` is opaque `object?`; Core does not interpret its business type or value domain. Delivery's `contentType` describes the representation:

- Runtime representation uses ordinary CLR objects.
- Encoded representation uses `System.BinaryData`.
- `contentType` identifies representation, not the application CLR type. Event ID and application contracts provide business identity.

Media Type syntax follows RFC 9110's type/subtype and parameters, with no Eventa-specific grammar. A CLR runtime representation may use an Eventa vendor media type. `application/vnd.eventa.runtime-value; model=clr` is only a non-normative placeholder, not a settled or registered protocol value. [RFC 9110 §8.3.1](https://www.rfc-editor.org/rfc/rfc9110.html#section-8.3.1)

`contentType` is a new Delivery field to be adopted jointly by Eventa implementations, not a TypeScript 1.0.2 field. Old frames require the explicit compatibility profile described in [§4.7](#47-compatibility-layers).

#### Authoritative representation metadata

Delivery's `contentType` is authoritative. `BinaryData.MediaType` is representation-local metadata, not a second protocol fact. Create, Restore, and Core routing must not use it to fill, override, infer, or reject a Delivery. Non-Core diagnostics/helpers may report discrepancies without changing protocol interpretation.

#### Stable ownership and sharing

A retained body and its backing storage must remain valid and observably stable for as long as any consumer may use the Delivery. Producers and consumers must not mutate it; when that cannot be guaranteed, snapshot first. Stabilized CLR objects and BinaryData use ordinary GC-managed lifetime.

BinaryData does not automatically copy supplied memory. Its byte-array and `ReadOnlyMemory<byte>` constructors, and `FromBytes`, wrap existing storage; `ToArray()` explicitly copies. Reusable network buffers, PipeReader data, ReadOnlySequence data, and pooled memory must be copied or otherwise stabilized before they become invalid. [Microsoft BinaryData](https://learn.microsoft.com/dotnet/api/system.binarydata?view=net-10.0-pp) [.NET 10 BinaryData source](https://github.com/dotnet/runtime/blob/v10.0.0/src/libraries/System.Memory.Data/src/System/BinaryData.cs)

Local consumers, forwarding, and fan-out share the same stable Eventa/body references without per-branch copies. P0 introduces no IMemoryOwner, manual lease, reference counting, or pooled Delivery lifetime protocol. Detailed buffer, snapshot, and queue-lifetime rules are in [Research 4 §2.2–2.3](research-4-eventa-body-codec-boundary.md#22-object-and-binarydata).

#### Missing and null

Core assigns no business meaning to missing body versus null and adds no `BodyPresence` abstraction. Application protocols may distinguish them. Profiles promising representation preservation retain the states they support; Invoke may define presence rules inside its own control structure.

### 4.3 Core ingress, deduplication, and listener completion

#### Application and Adapter handoff

Core provides the Delivery model, duplicate suppression, local routing/dispatch, and pure `Forward()` derivation. Application is a responsibility carried by user code: it composes Adapters, delivery targets, filters, relay, fan-out, and representation conversion. P0 requires no `EventaApplication`, `Application`, or central facade/runtime owning Adapter and Context lifecycles. An optional helper may be added for demonstrated repeated glue code, but cannot become a Core prerequisite.

The inbound sequence is:

```text
Adapter/profile
    ↓
framing / size / trust / envelope / ownership validation
    ↓
IDeliveryLike
    ↓
Delivery.Restore(...)
    ↓
opaque, Core-valid Delivery
    ↓
Application orchestration
    ↓
IDeliveryTarget
    ↓
Core dedupe
    ↓
local routing / dispatch
    ↓
selected endpoint/listener
    ↓
optional typed decode
```

Inbound Adapter/profile responsibilities end with carrier/framing, resource/size/trust validation, envelope decoding, and transient-memory stabilization. They provide `IDeliveryLike`, establish a valid opaque Delivery with Restore, and hand it to Application. Retained-carrier handoff is defined in [§4.6](#46-retained-carrier-representation-and-transparent-relay). An Adapter does not select EventContext, depend on or call the Core target directly, inject Core, choose drop/local/relay/fan-out policy, call Forward, or convert representations.

Application makes opaque-envelope decisions: drop, local target, relay/fan-out, and outbound representation policy. It may explicitly convert outbound or relay branches. For local consumption it must not decode or materialize business payloads before Core deduplication/routing. A local branch can retain a CLR object while Application builds JSON BinaryData for WebSocket. The typed boundary is in [§4.4](#44-typed-helper-boundary).

An Adapter accepts the representation Application selected and owns only its carrier, framing, transport lifecycle, and declared host behavior. It has no automatic converter registry, conversion graph, target selection, or capability negotiation. Unsupported representations fail explicitly. It may materialize a whole carrier or extract envelope fields and retain an encoded body as BinaryData; reusable, returned, or expiring transport memory must not escape into a retained Delivery.

#### `IDeliveryTarget` capability

The public ingress capability is:

```csharp
public interface IDeliveryTarget
{
    Task AcceptAsync(Delivery delivery);
}
```

`AcceptAsync` submits an already-valid Delivery to the current Core/EventContext for deduplication, local routing, and dispatch. Its main caller is Application orchestration. It accepts neither `IDeliveryLike` nor retained carrier state; Restore has already validated the nullable structural view.

An EventContext implementation provides two parallel capabilities: `IEventContext` for application-facing emit/subscribe/lifecycle, and `IDeliveryTarget` for existing-Delivery ingress. `IEventContext` does not inherit the target interface, so ordinary event consumers do not automatically receive raw injection capability.

Because completion tracks asynchronous listener work, the operation follows TAP naming and returns Task. No benchmark or demonstrated high-frequency use justifies ValueTask's added complexity. Duplicate and zero-listener paths may return `Task.CompletedTask`. Task supports storage, repeated awaits, and caller composition with `Task.WhenAll`/`Task.WhenAny`; none of these caller choices changes Core's fail-fast completion contract.

#### Submission and cancellation boundary

Calling AcceptAsync formally submits the Delivery to Core. The interface has no caller-supplied CancellationToken. Application may check its own token before calling the target; once acceptance begins, the caller token does not participate in that Delivery's Core lifecycle.

Treating caller cancellation as cancellation of the whole Delivery after pre-dispatch registration would conflate acceptance with business outcomes, so P0 provides no such path. Context shutdown/lifetime cancellation is separate: it may later affect listener/Invoke work but must never roll back recent-seen entries. Its mechanism is not defined by this ingress signature. Typed and Invoke APIs may still accept their own CancellationTokens.

#### Local acceptance and next-hop forwarding

A local .NET target performs duplicate suppression, routing, and dispatch without calling Forward or decrementing hops. This explicitly separates TypeScript's internal continuation bookkeeping into local acceptance at the current node and Application-controlled next-hop derivation. Only deriving a forwarding branch consumes a hop; the public API need not copy TypeScript's internal accept.

The same inbound Delivery can be submitted unchanged to a local target while Application independently derives relay branches from the original. Local consumption does not change the branch's starting budget. Hop 0 still allows local acceptance/dispatch. Construction and exhaustion rules are in [§4.1](#41-delivery-and-eventa-model); Adapter admission/send also leaves hops unchanged.

#### Recent-delivery history

With local acceptance separated from forwarding, Eventa.NET intentionally adopts TypeScript 1.0.2's observable duplicate-suppression and listener-completion semantics using .NET Task primitives.

Before local routing/dispatch or any application listener, typed endpoint/helper, or typed decode runs, Core must atomically check and register the `deliveryId` as recently seen. `TryMarkRecentlySeen(deliveryId)` expresses this concept, not a settled method name. Separate Contains/Add operations must not leave a concurrency gap that admits the same ID twice.

Defaults are a five-minute TTL and capacity of 10,000 delivery IDs. This is bounded history for the current routing domain, not a permanent deduplication database. An expired or capacity-evicted ID may be treated as unseen and dispatched again. Storage, cleanup, lazy/eager expiration, synchronization, and eviction algorithms are implementation choices; no particular lock, collection, generation set, or cache is required.

#### Seen does not mean successfully processed

Recently seen means Core accepted the logical Delivery into routing, not that all listeners succeeded: `seen != successfully processed`. Listener, endpoint, and typed-decode failures must not delete or roll back that entry. Core is not a transactional inbox, business retry engine, or exactly-once processor. Application or higher protocols own retries rather than automatically making a failed ID unseen.

A first delivery with no matching listeners still succeeds and stays recently seen. Registering a listener later does not permit the same ID to dispatch again while its history entry remains valid.

#### Duplicate completion

A retained ID is suppressed before routing/dispatch and completes as a successful no-op without invoking listeners, typed endpoints, or helpers. A duplicate neither waits for nor joins/coalesces the first processing operation and does not inherit its success or failure. Core needs no `deliveryId → in-flight Task` table or separate in-flight deduplication state.

A duplicate can therefore succeed while the first listener is still running, even if that first operation later fails. The guarantee lasts only while the ID remains in recent history; it is not permanent or unlimited in-flight suppression.

#### Listener invocation and completion

Every matching listener must get an invocation opportunity. A synchronous throw is captured as a listener failure rather than ending enumeration; later listeners must still run. Capturing it or representing it as Task failure is an implementation detail.

On success, target completion waits until all invoked listener work succeeds. Once all matching callbacks have had their opportunity, an observed failure must fail completion without waiting for other started, pending listeners to become terminal. An all-settled failure contract is not allowed. Failure does not automatically cancel those listeners.

After outward failure, Core must still observe every remaining listener Task's terminal state/fault to avoid unobserved Task exceptions. Observation must not delay outward failure until all Tasks end. No particular BCL helper is required.

The listener Exception selected by fail-fast aggregation propagates unchanged to the caller. `await AcceptAsync(...)` observes that exception. Core adds no mandatory EventaDispatchException or Eventa-specific wrapper, and defines no AggregateException contract collecting all listener failures.

Concurrent or near-simultaneous failures have no stable selection order, particularly no registration-order winner. JavaScript rejection reasons can be arbitrary values; .NET failures use Exceptions. No wrapper or JavaScript microtask timing emulation is needed. Later faults are observed but cannot replace or change the completed outward failure.

#### Routing diagnostics and later faults

Eventa.NET P0 exposes no counterpart to TypeScript's `routing.onDiagnostic`/`hops-exhausted`: no `Action<RoutingDiagnostic>`, .NET event, or copied callback-failure effect on acceptance. This does not change hop-0 local dispatch.

Observing later listener faults is internal lifecycle correctness, not a requirement for public callbacks, EventSource, ILogger, events, or Actions. P0 adds no later-fault diagnostic subsystem. Future routing or fault observability is demand-driven, outside the current contract, and need not resemble the TypeScript hook.

### 4.4 Typed helper boundary

Application or an application-owned typed endpoint explicitly calls a typed helper to convert between application types and body representations. Core, Adapter, and Invoke do not orchestrate that conversion.

Outbound conversion occurs before Adapter submission; branches of one logical Delivery may choose different representations. Inbound business decoding occurs only after §4.3 deduplication/routing, in the selected endpoint/listener, using event ID, active invocation, and contentType to choose a contract. Framing, size/trust validation, and memory stabilization are not business decoding and may precede Core.

A typed projection is a local consumption view, not a replacement for the immutable Delivery. Decode failure follows the listener/endpoint failure path: the first operation fails without rolling back seen; retained-history duplicates do not decode again. Unknown events still route locally, and Application relay does not depend on a typed registry.

Application-owned helpers/endpoints own the registry, schemas/codecs, and AOT metadata. Registry lifetime may match EventContext lifetime without transferring storage or ownership into Core. Keep the helper interface small and stable. [Research 4](research-4-eventa-body-codec-boundary.md) covers serializers, source generation, JSON numbers/dates/bytes/errors, and missing/null, with JSON rules remaining P1 candidate input.

### 4.5 Invoke protocol contract

#### Protocol detection and representation

When semantic Media Type parameter lookup finds `protocol=invoke`, Invoke interprets correlation, stream state, terminal fields, `invokeId`, `isReqStream`, and frame-specific structure in the complete body. Without that parameter, the Delivery is an ordinary event.

Detection must use a Media Type parser, not substring matching, fixed parameter order, or full-string ordinal equality. Compare type/subtype and parameter names by Media Type rules. Parameter-value case sensitivity, unknown values, and applicable media profiles are profile-defined; values must not all be assumed case-insensitive.

There is one body representation:

- `contentType` describes the complete Invoke body.
- Invoke reads control fields and uses event IDs to distinguish send/error/stream-end/abort/receive frames.
- `content` is an application/platform value inside that representation, with no nested Eventa content type.
- A CLR runtime body can hold a CLR Content object; a JSON body holds a JSON `content` value.
- Application owns representation conversion. Generic routing does not interpret Invoke mismatches.

Representations of send-error, receive-error, and abort reasons belong to the Application/helper profile.

Invoke only needs to parse its control structure in the Delivery's representation. It does not select the business-content codec, schema, or target representation, and does not call typed helpers implicitly. Application or an application-owned typed endpoint makes those calls.

#### Frame shapes and correlation

Invoke follows the seven event IDs, control fields, and error nesting in [§2.6](#26-invoke-control-bodies-and-classification-markers), including send-error. Every request-stream chunk carries `isReqStream: true`. Response, response-error, and response-stream-end use dynamic IDs ending in `invokeId` and verify the body ID. The local endpoint silently ignores mismatches; generic Core and transparent relay do not inspect them.

C# adds neither a numeric InvokeEventType wire contract nor a separate Invoke protocol version. Application-facing APIs use `EventDefinition<T>`, `Task<T>`, `IAsyncEnumerable<T>`, and CancellationToken for definitions, unary results, streams, and cancellation. These do not change Core ingress in §4.3.

#### Handler registration identity and disposer lifecycle

Eventa.NET maps TypeScript handler function identity to normal .NET delegate equality. Registration identity is scoped by:

```text
EventContext
+ invoke request/send event ID
+ handler delegate equality
```

This is CLR-local registration bookkeeping, not a cross-language wire identity. Duplicate detection uses delegate equality, not delegate object reference identity or `ReferenceEquals(handlerA, handlerB)`.

An equal delegate registered under the same Context and request/send event ID reuses the active effective registration. It creates no second set of protocol listeners/state and must not cause a second handler call for one invocation. Unequal delegates remain distinct responders under the multi-responder contract below.

The request/send event ID is the equivalent of `event.sendEvent.id`, used by the TypeScript 1.0.2 registry. CLR definition-object identity, delegate reference identity, and other derived identities must not replace it.

Registration lifecycle rules are:

1. Duplicate registrations share one active registration generation. Generation is an internal semantic concept, not a required public type/API.
2. Any disposer for that generation can retire it. Retirement closes admission of new invocations; already-materialized invocations retain their continuation paths until they settle.
3. Other disposers from the retired generation become safe no-ops. Repeated calls to any disposer also have no additional effect.
4. An equal delegate can be registered again after removal, creating a new active generation.
5. A stale disposer must not remove or affect a later generation.

In this conceptual example, A and B hold disposal capability only for generation #42:

```text
register H → generation #42 → disposer A
register equal H → same generation #42 → disposer B
A disposes → #42 removed
register equal H → generation #43
B disposes → stale #42 disposer → no-op; #43 remains active
```

Registration disposal is separate from invocation cancellation. Unregister/dispose closes new-invocation admission without automatically cancelling materialized or started invocations.

An invocation materialized before disposal keeps its continuation routing. Later request-stream chunks, send-error, stream-end, and explicit abort for that ID must still reach its original state. Context lifetime/death continues to apply until it settles. A new ID first encountered after disposal must not be materialized by the retired registration.

If an equal handler is re-registered while an old generation still has active invocations, those existing IDs remain exclusively with their original invocation state. The new generation must not materialize them again. Only IDs not previously materialized for that handler registration identity may enter the new active generation. Distinct handlers may still independently materialize state for the same invocation ID.

Registration and invocation lifetimes are therefore separate. A retired registration's existing states may remain alive until their invocations complete, abort, fail, or end through Context death; only then can those states be cleaned up. Disposal need not await them. This introduces no DisposeAsync, drain timeout, or forced termination of user code.

The Invoke registration layer determines which effective listeners exist. Core's listener store does not deduplicate Invoke handler registrations; dispatch still invokes every actual matching listener as required by §4.3.

The retired-registration continuation rule is an intentional CLR-local Eventa.NET guarantee, not a claim about TypeScript 1.0.2. TypeScript disposal can detach active request streams from protocol listeners or Context-abort participation. Eventa.NET does not reproduce that lifecycle gap. This difference changes neither wire frames, correlation, nor cross-language protocol semantics.

#### Multi-responder semantics

Eventa.NET intentionally preserves TypeScript 1.0.2 behavior for multi-responder registration and caller completion, without a .NET-specific single-responder restriction.

An existing handler for a request/send event ID must not prevent registration of another distinct handler. Invoke imposes no single-responder uniqueness invariant, no required InvalidOperationException for a second registration, and no first-registered ownership of the ID. Distinct handlers may process the same invocation.

For unary calls, the first valid correlated response or error observed by the caller settles the invocation. Response first means success; error first means failure. Selection is not first-success, registration-order, or handler-priority based. Later outcomes cannot change a completed invocation.

A response stream may combine correlated chunks from multiple responders in actual caller-observed arrival order until its first valid correlated terminal. The first stream-end completes the whole caller stream; the first error terminal fails it. Later chunks/end/error frames cannot affect completion. The first chunk does not bind the stream to a responder.

There is no deterministic global ordering between responders. Each preserves chunk-before-own-terminal ordering. For observed arrivals `A1 → B1 → A2 → B2 → END-A → END-B`, the caller receives the first four chunks in that order, completes at END-A, and ignores END-B.

Unary settlement or a winning stream terminal does not implicitly cancel other responders. They may continue, but cannot alter the completed caller result. Explicit Invoke abort and Context lifetime remain the cancellation mechanisms.

Normal Application topology should have one effective handler at a time within one Context. Single-effective-responder selection, load balancing, deterministic selection, routing, and middleware are Application/routing policies, not base registration invariants.

#### Ordering, abort, and lifecycle

Independent emits have no global order. A request stream containing data, and each responder's own response stream, send their chunks before their own end/error terminal. Cancellation is an independent routed event and may overtake in-flight data.

Zero-chunk/terminal-first requests are valid. Stream-end or send-error may be the first request-stream frame for an `invokeId`. Endpoints must accept it under normal invocation correlation, not reject it as malformed or unknown solely because no chunk preceded it.

One unified, bounded InvokeAbortTracker correlates early abort. An abort for an unknown ID is recorded first. Within each handler's invocation state, the first later request materializes at most one cancelled invocation and subsequent frames are suppressed. This does not limit distinct responders. P0 retains 1,024 records and evicts the oldest. This local resource policy is not a wire field and is separate from Core's 10,000-ID recent history.

Context death ends pending clients, cancels active handlers, and terminates request streams without inventing a remote single-invocation abort frame. Removing pending listeners suppresses late frames. CLR cancellation, async streams, locks, and containers can use native implementation patterns rather than copy TypeScript internals.

#### Tracker bookkeeping and observability

TypeScript 1.0.2 has no tracker-specific diagnostic surface, and Eventa.NET has no current demand for one. P0 adds no tracker onDiagnostic, tracker event/logger hook, eviction callback, late/ignored-frame callback, or speculative public observability API.

Early-abort retention, capacity eviction, ignored frames, late-frame suppression, and completed/unknown invocation-state cleanup are internal bookkeeping. They do not introduce additional effects on normal Invoke completion. Future observability can be designed for demonstrated needs without reserving an API shape now.

This is separate from §4.3's Core decision. TypeScript has a routing.onDiagnostic/hops-exhausted hook that Eventa.NET P0 does not copy; TypeScript has no Invoke tracker counterpart, and Eventa.NET does not add one. Tracker diagnostics are not an Invoke version of the routing hook.

Research 4 covers JSON errors, abort reasons, business-content codecs, and physical parsing layouts.

### 4.6 Retained carrier representation and transparent relay

#### Fixed Core model and profile fields

The fixed Delivery/Eventa model has no Extensions dictionary, JsonExtensionData, or generic unknown-field storage. Local creation emits neither of these two groups:

- Known non-Core Eventa properties: TypeScript runtime `type`, `metadata`, and `invokeMetadata`. Excluding them from C# Core does not make them private Adapter fields.
- Unknown or Adapter/profile/host-specific extras: unknown properties, beta.13 aliases, timestamp wrappers, Worker transferables, window-message wrappers, Electron/Tauri host information, and remote-method stubs.

Internal platform metadata can remain self-consistent without reproducing TypeScript's local structures.

#### Carrier storage and lifetime

A bridge/transparent-relay profile retains the original carrier, or equivalent profile-native representation, while supplying a Core-valid Delivery projection. Retained carrier state means that representation, not a generic extracted extras bag.

```text
Profile-decoded carrier
├── Delivery projection
└── retained carrier representation  ← original carrier or equivalent profile-native representation
```

IDeliveryTarget receives only the Delivery projection. Application/profile carriers hold retained state outside the fixed model, target, and routing Core, so Core need not interpret, dismantle, or remodel profile content.

Endpoints that do not relay may discard retained state. Transparent relay must maintain its association with the Delivery projection until re-encoding. Stable ownership follows §4.2. Concrete carrier/public interfaces, representation types, and profile-specific lifetime/failure contracts remain in §6.

#### Composing forwarding

Application can use both a local endpoint and a relay branch. The local target receives the original Delivery; relay derives a next-hop Delivery with `original.Forward()` and keeps it associated with retained carrier representation.

Core Forward only derives Delivery and does not process retained state. A profile claiming transparent relay must provide or require a re-send path based on that representation, applying the forwarded Delivery's routing changes. Extracting `carrier.Delivery.Forward()` and discarding retained state cannot preserve content Core does not own or understand.

Where Adapter A → Adapter B requires a new representation, Application explicitly invokes a known conversion. Arbitrary host values cannot be assumed convertible to arbitrary values in another language.

#### Representation preservation

Except where the protocol requires changes, profiles should preserve the original representation of untouched carrier content rather than actively normalize, discard, or rewrite it because it passed through Core. This does not require retained storage inside Core.

The guarantee is stronger than semantic equality alone, but does not require a byte-exact round trip of the entire carrier: forwarding changes routing state. JSON raw-token preservation is one profile implementation, not a universal Core encoding requirement.

### 4.7 Compatibility layers

#### Core semantic compatibility

Eventa.NET preserves the protocol-visible and lifecycle semantics explicitly required here. CLR public APIs and internals need not match TypeScript, subject to the intentional platform differences recorded in this document.

Sections 4.1–4.6 define delivery identity, routing, hops, deduplication/dispatch, Invoke, and relay. TypeScript's internal accept continuation is split into local acceptance and explicit Forward. Section 4.3 preserves listener completion while intentionally omitting the local routing diagnostic hook.

Listener storage, CancellationToken/Source, async streams, locks, queues, and proactive late-response suppression may follow CLR patterns if required frames, ordering, correlation, at-most-once behavior, and cleanup outcomes remain intact.

#### Adapter wire compatibility

Wire compatibility covers carrier, framing, outer wrappers, body representation, size limits, and host lifecycle. TypeScript WebSocket JSON and a future .NET WebSocket JSON Adapter, for example, need a dedicated compatibility profile.

New C# encoders do not emit beta.13 aliases. The new decoder requires explicit valid `contentType` and current `deliveryId/hopsRemaining/eventa`, with no implicit legacy-only fallback, protocol negotiation, or alias-conflict resolution. Vanilla TypeScript 1.0.2 combined frames lack contentType and cannot be restored unconditionally. An explicit profile must establish body representation, construct contentType, and map Invoke markers before restoration. It may ignore `id/type/payload/timestamp` at an endpoint or retain them externally for relay according to its preservation contract. This profile is outside Core-first P0.

#### Typed body compatibility

Both peers must use the same schema and codec for a business event ID, such as agreed JSON decimal/date/error rules or a Protobuf schema.

Typed end-to-end interoperability requires all three layers. Core compatibility does not guarantee every TypeScript Adapter or arbitrary JavaScript body will work with a C# typed API.

### 4.8 .NET Adapter admission, backpressure, and reconnect

#### Transport and Application boundary

Adapters carry the representation Application selected. They do not implicitly convert business values, select local targets, or provide remote acknowledgement. Ordinary publication and successful admission prove neither remote receipt, processing, nor persistence. Invoke or another higher protocol provides request/response lifecycle.

Concrete WebSocket JSON, Unix newline, Worker clone/transfer, window-message, Electron IPC, and Tauri capabilities are implemented independently for business needs under §5. Core has no global JsonSerializerOptions, ProtoJSON value domain, or JSON frame limit; individual Adapters/helpers own codecs and resource settings.

Public APIs use .NET TAP, Task/ValueTask, CancellationToken, and events rather than copying TypeScript interfaces. The following architecture semantics are fixed; concrete types and interface choices remain in §6.

#### Buffering and backpressure

Core provides no unified queue, buffer, or backpressure model. Adapters buffer only where transport or flow control needs it, preferably using existing framework mechanisms. Adapter-owned queues must be bounded, have reasonable defaults, and support capacity configuration.

Adapters hide transport-specific send serialization. Application may submit concurrently; a WebSocket Adapter can serialize connection sends with an async mutex, semaphore, bounded channel, or send loop. Callers need not compensate for transport concurrency constraints.

Retry, drop, coalescing/keep-latest, durable queues, batching, priority, and accumulating new business messages while disconnected are Application policies. Base Adapters do not default to DropOldest/DropNewest, infinite retry, persistence, or business-message merging when full.

#### `SendAsync` and `TrySend`

These are conceptual operations; concrete interfaces may choose parameter types and Task/ValueTask forms while preserving their semantics:

```csharp
SendAsync(delivery, cancellationToken)
TrySend(delivery)
```

SendAsync asynchronously waits for admission capacity. When connected with capacity, it completes after taking ownership; when full, it waits without blocking a thread and allows CancellationToken cancellation while waiting. Success does not mean remote receipt, processing, or persistence.

TrySend attempts immediate admission. It returns true only when ownership is accepted, false otherwise. False guarantees no Adapter ownership and no partial enqueue or inconsistent handoff.

#### WebSocket reconnect and queued ownership

A base WebSocket Adapter may own bounded transient-reconnect policy, including retry limits and delay/backoff. This is not a reliable replay protocol.

Connected Adapters use normal admission. Reconnecting, terminal-failed, and closed Adapters reject new admission: TrySend returns false and SendAsync immediately fails as unavailable/failure rather than waiting for reconnect. Application decides whether to retain new business deliveries and resubmit them after a state change.

Previously admitted deliveries whose transport send has not started may stay in the bounded queue through transient reconnect and receive their first send after recovery. The first transient disconnect does not clear them. Exhausted reconnect attempts cause terminal failure and release remaining queued ownership; silent queue.Clear is not allowed. Concrete interfaces determine pending counts and per-item failure details.

#### Base WebSocket at-most-once behavior

Distinguish queued from in-flight deliveries. Queued items have not started a send and may be sent for the first time after reconnect. In-flight items have started a send whose remote result may be unknown, so the base Adapter does not replay them. Completed sends are not replayed either.

The base Adapter adds no application ACK, replay, sequence, persistent outbox, exactly-once, or at-least-once protocol. Stronger guarantees belong to a specialized transport/profile or Application protocol with ACKs, sequencing, gap detection, snapshots, or reconciliation, not Core.

#### Lifecycle observation

Adapters expose state through UI-framework-neutral .NET APIs:

```csharp
AdapterState State { get; }

event EventHandler<AdapterStateChangedEventArgs>? StateChanged;
```

Conceptually, AdapterStateChangedEventArgs contains OldState, NewState, and nullable Error. Error describes only the exception causing that transition: null for success, terminal or last relevant error for terminal failure. There is no permanent LastError property. WPF, WinUI, CommunityToolkit MVVM, and Rx applications can project this API into their own binding models without adding those dependencies to the Adapter.

Lifecycle includes disconnected, connecting, connected, reconnecting, failed, and disposed phases. Exact enum values and transitions are concrete interface choices. Typical transitions are Connected → Reconnecting with the triggering error, Reconnecting → Connected with null Error, and Reconnecting → Failed with a terminal/last relevant error.

The Core/Application/Adapter boundary depends only on these architecture contracts. Concrete types, enums, exceptions, receive interfaces, reconnect options, and shutdown behavior do not block Core.

## 5. Implementation scope and dependency order

### 5.1 Core-first sequence

Implement in this dependency order, using §4 as the canonical behavior contract:

1. Delivery/Eventa model and Create/Restore (§4.1).
2. Core ingress, local routing, listener dispatch/completion (§4.3).
3. deliveryId and bounded recent history (§4.3).
4. hopsRemaining and Forward (§4.1, §4.3).
5. Opaque body, authoritative contentType, stable ownership (§4.2).
6. External retained-carrier boundary around Core projections (§4.6).
7. Invoke on the new Delivery/Core model (§4.5).
8. Typed helpers and concrete Adapters (§4.4, §4.7–4.8).

Steps 1–6 are Core P0 and can be implemented from the settled contracts. Section 6.1's parser, containers, synchronization, and Task composition are implementation choices, not uncertainty about observable behavior. Step 7 is Invoke P0, including the settled protocol, multi-responder, and registration identity/lifecycle contracts.

### 5.2 P0 deliverables

Core P0 delivers the model, valid-Delivery ingress, duplicate suppression/routing/dispatch, explicit next-hop derivation, and body/retained-carrier boundaries. Minimal test seams and user-owned glue code validate composition; no central Eventa Application runtime is required.

Invoke P0 delivers seven frame kinds, dynamic correlation, stream ordering, terminal handling, multi-responder behavior, handler registration identity/lifecycle, early abort, late-frame suppression, and Context-death cleanup. Application/typed endpoints retain business-content representation policy.

P0 does not implement concrete Adapter wire codecs, queues, backpressure, or reconnect. Concrete retained-carrier interfaces and Adapter details remain deferred without expanding the fixed Core model.

### 5.3 P1 typed helpers and concrete Adapters

Implement only Adapters needed by target applications. Each profile follows §4.8 and separately defines its carrier/wire format, frame limits, failure policy, retained representation, and end-to-end interoperability tests.

Typed helpers provide explicit schema/codec registration and source-generation/AOT paths. Research 4's Web-compatible JSON profile is candidate input requiring business and interoperability validation. Other helpers may use Protobuf, MessagePack, or host-native values.

TypeScript 1.0.2 contentType/Invoke mapping and beta.13 alias encoding/decoding are §4.7 Adapter compatibility work, not Core P0.

### 5.4 Excluded work

- Legacy-only beta.13 fallback and protocol negotiation.
- Reproducing every TypeScript Adapter or host feature, including Worker transfers and remote methods.
- Making JSON, structured clone, or CLR object graphs the only Core body model.
- Global cross-carrier conversion, Core-owned target selection/capability discovery, or automatic converter graphs.
- Core-owned queues, unified backpressure/business buffering, or mandatory Application runtime/lifecycle orchestration.
- A P0 routing.onDiagnostic hook or speculative public later-listener-fault diagnostics.
- Base WebSocket ACK/replay, persistent outbox, at-least-once, or exactly-once protocols.

## 6. Implementation choices and open interfaces

Only unsettled implementation and interface choices appear here. Settled contracts remain in §4.

### 6.1 Core implementation choices

Choose a Media Type parser/library, validation and parameter-lookup helper organization, existing Nano ID package calls, construction exception messages, and repository-appropriate files/namespaces.

Recent-history storage, atomic synchronization, expiration/cleanup, and eviction algorithms remain implementation choices. The representation of synchronous listener failures and internal composition of fail-fast completion with later-fault observation are also unspecified.

### 6.2 Retained carrier interface

Define how profiles hand Delivery projections and retained carrier representation to Application: public visibility, concrete retained types, profile-specific ownership/lifetime, forwarding/re-encoding, and failure interfaces.

### 6.3 Concrete Adapter interface

Define AdapterState members/transitions, supported representations, receive-side APIs, unsupported/admission failure types, reconnect options, graceful shutdown/dispose, and pending-count/terminal-failure reporting.

Concrete Adapters also choose final SendAsync signatures and Task/ValueTask return forms.

### 6.4 Raw-relay public interface

Decide whether to expose a raw-relay gateway API or retain internal composition only. A public API must define projection/carrier association, preservation levels, representation-conversion failure, and observable diagnostics.

## 7. Acceptance tests

These assertions follow §4. Core tests do not depend on concrete Adapters. Profile, relay, and helper tests run only when those features are implemented.

### 7.1 Eventa and Delivery construction

See [§4.1](#41-delivery-and-eventa-model).

1. Eventa is an ordinary sealed class with get-only id/body and `Eventa(string id, object? body = null)`. Null ID throws ArgumentNullException; empty/whitespace IDs are valid; omitted body is null. There is no metadata/extension storage, record equality, or with semantics.
2. Delivery is a non-positional sealed record implementing IDeliveryLike, with private constructor, public DefaultHopsRemaining = 32, get-only protocol properties, and the settled signatures. Initializers or field-changing with expressions cannot bypass invariants. Empty with copies remain valid and share references/identity.
3. Create generates a non-empty Nano ID, defaults hops, and preserves Eventa/body references. Callers cannot choose ID/hops. Null Eventa and null/invalid contentType are rejected; tests add no generator DI.
4. Restore preserves values/references from any valid IDeliveryLike and returns an existing Delivery unchanged. Null argument throws ArgumentNullException; invalid members throw ArgumentException. Whitespace delivery IDs and empty/whitespace event IDs remain valid.
5. Restore does not generate/trim IDs, reset/clamp hops, canonicalize contentType, infer representation from body metadata, create/clone Eventa, or repair input. It need not repeat the Eventa constructor's non-null ID check.
6. Create/Restore share Media Type validation and preserve exact valid strings. Fractional/out-of-Int32 wire hops are decoder tests, not inputs expressible by IDeliveryLike.

### 7.2 Core ingress and Application composition

See [§4.3](#43-core-ingress-deduplication-and-listener-completion).

1. Ingress is exactly `Task AcceptAsync(Delivery delivery)`, not ValueTask, with no caller-token parameter/overload. EventContext implements parallel IEventContext/IDeliveryTarget capabilities without inheritance between them.
2. Targets accept only Core-valid Delivery, not IDeliveryLike or retained state. Restore owns structural validation.
3. After framing/size/trust/envelope/ownership validation and Restore, Adapters hand data to Application without calling a target, selecting a Context, or injecting Core.
4. Application may check a token before submission. Submission then follows Core semantics without caller-token withdrawal; Context lifetime cannot roll back seen. Tests require no new lifetime API.
5. Application selects targets/drop/relay/fan-out/conversion; user-owned P0 glue code needs no central facade/runtime.
6. Duplicate and zero-listener paths return successful Tasks. Task.CompletedTask is allowed, but no particular Task instance is required.

### 7.3 Recent-delivery history

See §4.3's atomic registration, retention, and duplicate contracts.

1. Unseen IDs are recorded before routing, listeners, endpoints/helpers, or typed decode. Concurrent ingress with the same retained ID admits only one dispatch.
2. Identity is deliveryId only, even when other record fields differ; record equality, whole-object hashes, and reference equality are not substitutes.
3. Duplicates succeed before routing without re-invocation, waiting for pending work, or inheriting the first outcome.
4. Listener/endpoint/decode failure does not roll back seen. A retained duplicate produces no additional decode or codec error.
5. Zero-listener first deliveries succeed and stay seen, even when a listener is registered before resubmission.
6. Defaults are five minutes and 10,000 IDs.
7. Expiration or capacity eviction permits dispatch again. Tests require no particular eviction algorithm, permanent deduplication, or unbounded in-flight suppression.
8. Duplicate completion does not coalesce/join work and requires no in-flight Task table/state.

### 7.4 Listener completion and failure

See §4.3.

1. A synchronous throw does not prevent later matching listeners from being invoked.
2. Success remains incomplete until all listener work succeeds.
3. After all callbacks get an invocation opportunity, failure propagates while other listeners may still be pending. Tests require no JavaScript microtask timing.
4. Outward failure does not automatically cancel started listeners.
5. Both synchronous and asynchronous failures propagate through the outward Task; awaiting yields the selected original Exception instance without forced wrappers or aggregate-all behavior.
6. A concurrent-failure winner need only be one actual listener failure. Tests assume neither registration-order selection nor aggregation of all failures.
7. Later faults remain observed without unobserved Task exceptions, changing the selected exception, or delaying completion into all-settled behavior. No public diagnostic subsystem is required.

### 7.5 Hops and envelope derivation

See §4.1 and §4.3.

1. Forward returns a new Delivery, leaves the original unchanged, decrements exactly once, and preserves ID, exact contentType, and Eventa/body references.
2. Fan-out branches derive independently from the same original; only chained forwarding decrements repeatedly.
3. Local acceptance/dispatch and Adapter admission/send neither decrement hops nor create implicit next-hop continuations. Local-consumption order does not change a relay branch's starting budget.
4. Hop 0 allows local dispatch; Forward throws InvalidOperationException. No static forwarding overload, TryForward, or CanForward is added.
5. Hop 1/0 and recent-history cycle suppression do not depend on body representation. P0 adds no routing.onDiagnostic/hops-exhausted callback/event or copied diagnostic-callback failure path.

### 7.6 Opaque bodies, typed boundaries, and retained-carrier relay

See [§4.2](#42-body-representation-contenttype-and-ownership), [§4.4](#44-typed-helper-boundary), and [§4.6](#46-retained-carrier-representation-and-transparent-relay).

1. Unknown events route without a typed registry, JsonTypeInfo, or codec delegate. Core does not inspect business types of CLR objects, stable BinaryData, null, or other opaque values.
2. Only selected endpoints decode after deduplication/routing. Typed projections do not replace Delivery; registries remain outside Core and are unnecessary for relay.
3. Transient/pooled/pipe-owned data is stabilized before retention and survives buffer reuse. Local use, awaits, queues, and fan-out share stable bodies without invalidation or observable mutation.
4. Application may keep CLR values locally and explicitly convert remote branches. Core/Adapters do not convert implicitly, and unsupported Adapter representations fail explicitly.
5. Missing/different BinaryData.MediaType cannot fill, override, infer, or cause Core to reject Delivery, or change protocol interpretation.
6. Core assigns no missing/null business semantics; each profile tests preservation of its declared states without accidental collapse.
7. Retained representation covers type/metadata/invokeMetadata and unknown outer/eventa properties. Endpoints may discard it; transparent relay keeps it associated with forwarded Delivery until re-encoding and applies only required protocol changes.
8. Core projection must not actively dismantle, normalize, or lose carrier content. Tests require neither a generic extras bag nor whole-carrier byte equality, and do not settle for semantic equality alone.
9. Vanilla TypeScript 1.0.2 cannot Restore without contentType. Explicit profiles separately test representation, contentType construction, Invoke mapping, and preservation.

### 7.7 Invoke control

See [§4.5](#45-invoke-protocol-contract).

1. Cover all seven frames, unary request/response, dynamic response IDs, and body invocation-ID correlation.
2. Cover request-stream isReqStream: true, terminal-first behavior, send-error, and chunk-before-terminal ordering.
3. Cover response streams, empty streams, handler failure, and late frames.
4. Cover early abort/overtaking, per-handler same-ID at-most-once behavior, bounded 1,024-entry tombstone history, oldest-entry capacity eviction, and cleanup/late frames.
5. Context death ends pending clients, cancels active handlers, and terminates request streams without inventing a single-invocation abort.
6. Only protocol=invoke enables control parsing; ordinary events do not enter the state machine.
7. Invoke uses one Delivery contentType, no nested content type, numeric InvokeEventType wire field, or separate protocol version.
8. Endpoints ignore dynamic/body ID conflicts; generic Core and relay do not inspect them.
9. Invoke does not choose business-content codecs or implicitly call typed helpers.
10. Semantic Media Type lookup handles type/subtype and parameter-name case, parameter order, and quoted values. Value comparison follows the profile; substring/full-string matching is insufficient.
11. With an active request stream in a retired generation, re-register an equal delegate. Existing-ID continuations must still reach only the old state; the new generation must not materialize that ID again, but must admit a previously unseen ID. This restriction applies within one registration identity and does not prevent distinct responders from handling the same invocation ID.

#### Handler registration lifecycle

1. Equal delegates under one Context and request/send event ID produce one effective responder and one call per invocation. Different Contexts or IDs do not share registration identity.
2. Unequal delegates remain distinct responders under unary/streaming multi-responder semantics.
3. Separate delegates targeting the same static method are duplicates under normal .NET equality, even without object reference identity.
4. Equal delegates for the same target and instance method are duplicates; do not use ReferenceEquals as the test oracle.
5. Duplicate-registration disposers share an active generation. Disposing either stops new materialization for it; the other becomes a no-op. Already-materialized invocations keep their retired-registration continuation behavior.
6. Equal delegates can re-register after removal in a new active generation.
7. Old disposers cannot remove or affect a new generation; new invocations must still be handled after the stale disposer runs.
8. Repeated disposal is safe and has no additional effect.
9. Disposal closes new admission without cancelling started invocations. At minimum, materialize a request stream, dispose its registration, and verify later chunks and terminal for that ID reach the original state and allow normal settlement. A new ID arriving after disposal must not materialize in that retired registration.
10. Explicit abort and Context death still reach active invocations of retired registrations. Context death cancels handler tokens and terminates request streams. Tests require no waiting Dispose, DisposeAsync, drain timeout, or forced termination of cancellation-ignoring user code.

#### Unary multi-responder

1. Two distinct handlers register for one request/send event ID and both process the invocation; an existing handler for that request/send event ID is not a reason to reject another distinct handler registration. Their responses share invocation correlation but have independent deliveryIds, so Core does not suppress them as one Delivery.
2. Response first succeeds despite a later error; error first fails despite another responder's later success.
3. Late response/error cannot change settlement. Invalid dynamic/body correlation cannot win.
4. The winner is the first valid correlated response/error observed, not registration order, priority, or first success.
5. Completion does not cancel the other responder, which may continue and finish.

#### Streaming multi-responder

1. Both responders can contribute correlated chunks to one stream; the first chunk does not bind its responder.
2. Pre-terminal chunks follow actual observable arrival order under controlled interleaving. Tests promise no fixed cross-responder order; each responder preserves its own chunk-before-terminal order.
3. The first valid stream-end completes the whole caller stream; later chunks/end/errors from another responder do not change it.
4. The first valid error terminal fails the whole stream; later frames cannot alter failure. Incorrectly correlated terminals do not end it.
5. The winner does not implicitly cancel other responders; explicit abort and Context lifetime remain separate.

### 7.8 Adapter profiles

See [§4.7](#47-compatibility-layers) and [§4.8](#48-net-adapter-admission-backpressure-and-reconnect). Run these only for implemented profiles.

1. Test carrier/framing, outer envelope, supported representation, unsupported-input failure, size/trust limits, transport failure, and TypeScript interoperability.
2. New encoders emit no beta.13 aliases. Explicit old-frame profiles follow endpoint-ignore/relay-preserve contracts without enabling legacy-only fallback.
3. Connected admission succeeds with capacity. When full, SendAsync waits asynchronously and can be cancelled; TrySend immediately returns false without partial enqueue/ownership.
4. Adapters hide send serialization for concurrent submissions; Application needs no external lock.
5. Owned queues are bounded with reasonable, configurable capacities and no implicit drop, coalescing, infinite retry, or persistence.
6. Reconnecting/failed/closed reject new admission; SendAsync does not wait for reconnect and TrySend returns false.
7. Transient reconnect retains queued ownership and sends for the first time after recovery. Terminal failure is observable and releases the remaining queue without silent clearing.
8. Completed or ambiguous in-flight items are not replayed. Base WebSocket claims no ACK, at-least-once, or exactly-once guarantee.
9. State/StateChanged old/new states and transition errors agree. Successful transitions have null errors; terminal failure exposes the relevant exception. No permanent LastError is maintained.
10. Inbound carriers/Delivery go to Application. Adapters do not call targets directly, select relay/fan-out targets, Forward automatically, or decrement hops.

Only JSON Adapters run JSON golden fixtures. Structured-clone and in-process Adapters are not constrained by JSON value-domain tests.

### 7.9 Typed helpers

Schema, JSON value-domain, remote-error, missing/null, unknown-property preservation, and AOT/source-generation tests belong to the applicable [Research 4](research-4-eventa-body-codec-boundary.md) profile, not Core routing.
