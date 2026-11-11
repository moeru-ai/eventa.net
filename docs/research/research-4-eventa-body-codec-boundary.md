# Eventa Body Codec Boundaries

> Updated: 2026-11-11
>
> TypeScript baseline: `@moeru/eventa` `v1.0.2`, commit [`5c1f576`](https://github.com/moeru-ai/eventa/tree/5c1f576ba8755241b56c68b9f2dc50feb6471d20)
>
> Scope: responsibilities of Core, Adapters, Invoke, and typed body codecs, plus implementation input for an optional JSON helper.

## Overview

This document explains how Eventa.NET retains, consumes, and forwards Delivery without interpreting business bodies, and where typed helpers perform explicit conversion.

Core uses `object?` for `eventa.body`: CLR objects represent runtime values; BinaryData represents encoded content. Delivery.contentType is the sole protocol authority. Bodies have stable GC-managed lifetimes, and consumption/fan-out share storage. Application composes local targets and Adapters; business decoding occurs only after Core selects an endpoint through deduplication/routing.

Section 1 records TypeScript 1.0.2 source facts. Section 2 defines C# representation and ownership boundaries. Section 3 is a P1 candidate JSON profile awaiting interoperability and business validation. Sections 4–5 cover acceptance tests and open interfaces. [Research 3](research-3-eventa-typescript-1.0.2-compatibility.md) is authoritative for Core models, ingress, recent history, listener completion, and Adapter transport contracts.

## 1. TypeScript 1.0.2 body handling

### 1.1 `EventaInner` is a minimal transport-neutral envelope

`EventaInner<T>` defines deliveryId, hopsRemaining, and eventa. `isEventaInner` validates minimum routing facts at those levels, including a string eventa.id, without reading eventa.body. [`internal.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/internal.ts#L16-L39) [`internal.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/internal.ts#L62-L76)

`Eventa<P>` has id and optional type/body/metadata/invokeMetadata. Its body type parameter defaults to unknown, not bytes; there is no Core JSON schema. [`eventa.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/eventa.ts#L52-L80)

The fixed contract is a minimum envelope, not a closed schema. Object spreads preserve other enumerable properties; invokeType, _flowDirection, and _workerTransfer illustrate runtime extensions. Unknown fields do not make isEventaInner reject a frame.

The current field is eventa. `payload` is a beta.13 compatibility alias; outbound helpers add id/type/payload/timestamp alongside the current three fields. The window-message Adapter also wraps EventaInner in its own outer payload field. That wrapper is Adapter-specific, not Core. [`adapters/internal.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/adapters/internal.ts#L7-L40) [`window-message/index.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/adapters/window-message/index.ts#L51-L90)

### 1.2 Generic routing does not decode business bodies

Internal accept in createContext is the routing/dispatch gate, not a public EventContext API. Public capabilities include emit/on/once/off/signal/abort. Internal accept deduplicates by deliveryId and, for positive hops, associates decremented continuation state before locally dispatching the complete eventa. Hop 0 still dispatches and reports a diagnostic. Dispatch selects listeners by event.id and match expressions; emit creates an event with a body before entering this gate. [internal routing/dispatch](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/context.ts#L102-L178) [public context surface](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/context.ts#L181-L307)

At hop 0, an optional routing.onDiagnostic callback reports hops-exhausted. Its exception joins accept's Promise.all failure path while local dispatch still runs. This is TypeScript's local hook behavior. [diagnostic callback](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/context.ts#L151-L168)

Channel forwarding hands the complete `Eventa<unknown>` to plugins. Without a plugin transform, the same body proceeds to the next Context; routing does not perform business decoding of payload properties. [`channel.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/channel.ts#L248-L301)

The TCP-proxy analogy applies to generic forwarding: a node recognizes and updates its routing header without understanding the application body. Envelope validation is still required; arbitrary unframed or unchecked input cannot simply be forwarded.

### 1.3 Invoke helpers parse control bodies

TypeScript places seven Invoke frame kinds' control fields in the Eventa body: invokeId, content, and optional isReqStream, with content.error for response errors. [`invoke-shared.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/invoke-shared.ts#L15-L47)

InvokeEventType is a numeric TypeScript enum. defineInvokeEventa stores it in enumerable invokeType, which JSON Adapters include when serializing the complete event. Type guards and the Tauri Adapter also use it. This implementation fact does not require the new cross-language protocol to fix its numeric values. [`defineInvokeEventa`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/invoke-shared.ts#L96-L136) [`isInvokeEventa`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/invoke-shared.ts#L143-L186) [Tauri adapter](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/adapters/tauri/index.ts#L40-L57)

Clients compare body.invokeId and read body.content or body.content.error, and construct these fields for requests. Handlers use invokeId/isReqStream/content to materialize invocations, push chunks, finish/fault request streams, and process abort. [`invoke.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/invoke.ts#L251-L307) [`invoke.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/invoke.ts#L521-L592)

Response streams read the same fields. The opt-in remote-method extension recursively walks payloads and recognizes its descriptors, another example of an upper layer interpreting bodies without expanding generic routing responsibilities. [`stream.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/stream.ts#L109-L145) [`invoke-remote-methods.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/invoke-remote-methods.ts#L101-L198)

Opacity describes the generic router, not the whole package:

- Core routing does not parse bodies.
- Invoke is a protocol layer above Core and parses its fixed control envelope.
- Typed helpers or business handlers decode Invoke content, request/response payloads, error details, and abort reasons.
- Transparent relay does not run Invoke helpers and need not parse control bodies.

### 1.4 Adapters own carrier serialization

JSON Adapters serialize the entire EventaInner. Native WebSocket uses JSON.stringify outbound and JSON.parse before restoring inbound data. Unix sockets also use JSON, adding LF framing, UTF-8, and frame size limits. [`websocket/native/index.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/adapters/websocket/native/index.ts#L24-L42) [`unix-socket/index.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/adapters/unix-socket/index.ts#L61-L103)

Browser Worker and BroadcastChannel submit JavaScript objects through postMessage. The HTML Standard specifies structured serialization during MessagePort sending and structured deserialization in the receiving realm. [`webworkers/index.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/adapters/webworkers/index.ts#L23-L44) [`broadcast-channel/index.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/adapters/broadcast-channel/index.ts#L44-L66) [HTML Standard: message ports](https://html.spec.whatwg.org/multipage/web-messaging.html#dom-messageport-postmessage-options)

Electron uses IPC directly; its documentation specifies Structured Clone serialization for arguments. [`electron/renderer.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/adapters/electron/renderer.ts#L43-L70) [Electron `ipcRenderer.send`](https://www.electronjs.org/docs/latest/api/ipc-renderer#ipcrenderersendchannel-args)

EventTarget and EventEmitter pass objects within a realm/process. Tauri passes the complete inner envelope to its host API. These paths do not share a universal Core JSON codec. [`event-target/index.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/adapters/event-target/index.ts#L31-L64) [`event-emitter/index.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/adapters/event-emitter/index.ts#L38-L52) [`tauri/index.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/adapters/tauri/index.ts#L40-L69)

Worker Adapters recognize Worker/transfer extensions, move transfer lists from the body into postMessage options, and reconstruct public body shapes on receipt. [`webworkers/internal.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/adapters/webworkers/internal.ts#L6-L37) [`webworkers/shared.ts`](https://github.com/moeru-ai/eventa/blob/5c1f576ba8755241b56c68b9f2dc50feb6471d20/src/adapters/webworkers/shared.ts#L42-L68)

Adapters therefore own language/host-specific carrier serialization, but do not all invoke an event-ID-aware typed body codec. Business-type conversion is a separate layer; otherwise every Adapter would need to understand every business type.

### 1.5 TypeScript 1.0.2 has no `contentType`

TypeScript 1.0.2 has no contentType or universal body-representation descriptor across Adapters. Carrier and host value domains determine what each transport supports. Adding contentType is a shared-protocol design beyond the existing 1.0.2 fields.

## 2. .NET representation and ownership contracts

### 2.1 Delivery envelope and Media Types

The transport-neutral model is:

```text
Delivery
├── contentType
├── deliveryId
├── hopsRemaining
└── eventa
    ├── id
    └── body
```

Delivery is a non-generic, non-positional, immutable sealed record implementing IDeliveryLike. Private construction and get-only properties protect routing invariants. Eventa is an ordinary sealed class with `Eventa(string id, object? body = null)` and only get-only id/body. IDs reject null but allow empty/whitespace strings. Core has no generic extension storage, and only protocol-facing members use camelCase.

IDeliveryLike exposes string? contentType, string? deliveryId, int hopsRemaining, and Eventa? eventa. A decoded carrier/profile can implement it directly. Restore validates invariants; Create generates a new ID and defaults hops to 32. Full signatures, validation, and reference-sharing rules are in [Research 3 §4.1](research-3-eventa-typescript-1.0.2-compatibility.md#41-delivery-and-eventa-model).

Vanilla TypeScript 1.0.2 lacks authoritative contentType and cannot Restore directly. An explicit compatibility profile must establish representation, construct contentType, map Invoke markers, and supply IDeliveryLike. Core guesses no default, and Restore neither understands nor repairs the old protocol. Profiles still own framing and carriers; the CLR model is not necessarily the final wire object.

Core interprets delivery identity, hop budget, and event identity. contentType describes the complete body representation; carrying it does not require decoding that body. It neither identifies application CLR types nor replaces event ID/application contracts.

Delivery stores contentType as a non-null string; IDeliveryLike uses string?. There is no Media Type wrapper/value object. Create and Restore share syntax validation and preserve valid strings without canonicalization. Semantic parameter lookup still requires a real parser, not substrings, fixed parameter order, or full-string equality. Parser/helper selection remains an implementation choice.

Media Types follow RFC 9110's type/subtype plus optional semicolon-separated name/value parameters. Type, subtype, and parameter names are case-insensitive. RFC 6838 states that order is insignificant and duplicate parameter names are errors; value syntax and meaning belong to the media type/profile. [RFC 9110 §8.3.1](https://www.rfc-editor.org/rfc/rfc9110.html#section-8.3.1) [RFC 6838 §4.3](https://www.rfc-editor.org/rfc/rfc6838.html#section-4.3)

`protocol=invoke` has valid token-form name and value, but requires a defined Eventa profile. It is not a universal parameter of arbitrary registered media types. For example, application/json registers no required or optional parameters: a parser may read `application/json; protocol=invoke`, but JSON does not define that parameter. [RFC 8259 §11](https://www.rfc-editor.org/rfc/rfc8259.html#section-11)

Where Eventa defines representation and parameter semantics, it should use a documented Eventa-controlled media type. RFC 6838's vnd. facet covers publicly available products. Subtype naming and registration details must be specified separately. [RFC 6838 §3.2](https://www.rfc-editor.org/rfc/rfc6838.html#section-3.2)

### 2.2 `object?` and `BinaryData`

Core uses opaque object? without a mandatory EventBody/OpaqueBody wrapper or parallel body-kind enum. Delivery.contentType describes:

- Runtime representation: ordinary CLR objects.
- Encoded representation: BinaryData.

BinaryData is the .NET byte-payload abstraction, backed by `ReadOnlyMemory<byte>`, with byte/stream/string/JSON conversion APIs. Its MediaType property must not become a second protocol authority. Core, Adapters, and receivers must not infer, override, or fill Delivery.contentType from it. [Microsoft `BinaryData` API](https://learn.microsoft.com/dotnet/api/system.binarydata) [.NET 10 `BinaryData` source](https://github.com/dotnet/runtime/blob/v10.0.0/src/libraries/System.Memory.Data/src/System/BinaryData.cs#L15-L66)

An encoded JSON body is a complete UTF-8 JSON value; a byte[] inside an application payload is business data that a typed helper may encode as base64 under §3.1. Serializing BinaryData as an ordinary byte[] property can change the wire shape. JSON Adapters need a raw-value path. Utf8JsonWriter.WriteRawValue requires a complete JSON value and validates by default. [Microsoft `WriteRawValue`](https://learn.microsoft.com/dotnet/api/system.text.json.utf8jsonwriter.writerawvalue?view=net-10.0)

### 2.3 Ownership, immutability, and fan-out

P0 uses stable GC-managed lifetimes: body and storage remain valid whenever any consumer can use the Delivery. It requires no public `IMemoryOwner<byte>`, manual lease, retain/release, reference counting, or pooled Delivery protocol.

BinaryData(byte[]), BinaryData(`ReadOnlyMemory<byte>`), and FromBytes wrap memory without copying. Adapters must copy or explicitly stabilize PipeReader, reusable socket, pooled, or borrowed data before invalidation/return. ToArray creates an independent array when ownership is uncertain; ToMemory does not. [.NET 10 `BinaryData` constructors](https://github.com/dotnet/runtime/blob/v10.0.0/src/libraries/System.Memory.Data/src/System/BinaryData.cs#L48-L178) [`ToMemory` / `ToArray`](https://github.com/dotnet/runtime/blob/v10.0.0/src/libraries/System.Memory.Data/src/System/BinaryData.cs#L393-L407)

Private construction and get-only fields prevent invariant-breaking initializers/with modifications; empty with copies remain valid. Create retains the supplied Eventa reference. Restore retains inbound Eventa/body and returns an existing Delivery directly. Neither object? nor `ReadOnlyMemory<byte>` implies deep immutability. Producers and consumers must not change observable body content after retention; snapshot first if needed. Once stable, local consumers, typed decoders, and outbound branches share Eventa/body without per-branch fan-out copies.

### 2.4 Application and Adapter orchestration

With representation and ownership established, Application chooses consumption and forwarding paths.

Application means user-owned orchestration, not a mandatory EventaApplication, Application class, or central facade/runtime owning Adapters/Contexts. Optional future composition helpers cannot be Core prerequisites. Core owns no target discovery, Adapter selection, converter registry, or conversion graph.

Outbound flow:

1. Core creates or accepts Delivery.
2. Application selects conversion, drop, filtering, or fan-out.
3. Application submits selected representations to one or more Adapters.
4. Adapters provide their carrier, framing, lifecycle, and host extensions.

Inbound flow:

1. Adapter/profile handles framing and validates frame size, trust, envelope, and ownership.
2. Given valid contentType, or one explicitly established by a compatibility profile, it supplies IDeliveryLike and uses Restore to establish a Core-valid opaque Delivery. Restore does not repair old protocol rules or invalid/missing members. Relay profiles retain the original carrier or profile-native state preserving untouched representation, alongside the Delivery projection.
3. Application selects local targets, drop, relay/fan-out, and outbound representation policy at opaque-envelope level. Local consumption must not require or trigger early business-payload materialization.
4. Application submits the original Delivery to a local target for deliveryId deduplication and routing/dispatch, preserving hops.
5. Only the selected application-owned typed endpoint/listener chooses a contract and decodes through its helper.

The public target capability is:

```csharp
public interface IDeliveryTarget
{
    Task AcceptAsync(Delivery delivery);
}
```

It accepts Core-valid Delivery, not IDeliveryLike or retained state. EventContext offers IEventContext's application API and IDeliveryTarget's existing-Delivery ingress as parallel capabilities without inheritance.

AcceptAsync has no caller-supplied CancellationToken. Application may check its own token before submitting; afterward Core deduplication/dispatch applies. Context lifetime is separate and cannot roll back seen. Typed/Invoke APIs may still use their own tokens.

Fail-fast aggregation propagates its selected listener Exception unchanged, with no forced wrapper or aggregate-all contract and no stable ordering between simultaneous failures. Core observes later faults without changing outward completion. P0 exposes neither TypeScript's routing diagnostic hook nor a public later-fault subsystem. Full completion rules are in [Research 3 §4.3](research-3-eventa-typescript-1.0.2-compatibility.md#43-core-ingress-deduplication-and-listener-completion).

Core atomically records recent-seen IDs before local routing/typed decode, with default retention of five minutes/10,000 IDs. Listener/decode failure never rolls back seen. Retained duplicates succeed independently; expired/evicted IDs may run again. Storage and full completion contracts remain in [Research 3 §4.3](research-3-eventa-typescript-1.0.2-compatibility.md#43-core-ingress-deduplication-and-listener-completion).

Adapters do not depend on/call targets directly, select Contexts, inject Core, Forward automatically, or convert representations. Application/profile carriers keep retained state outside Core. Local targets receive the original projection; relay derives original.Forward and maintains its association with retained representation.

Business decoding follows Core routing. An Adapter explicitly fails for unsupported contentType/body combinations instead of guessing how to serialize or decode runtime values.

For example, Application can retain CLR objects locally while building JSON BinaryData for WebSocket. Neither Core nor Adapter chooses that conversion policy implicitly.

[Research 3 §4.8](research-3-eventa-typescript-1.0.2-compatibility.md#48-net-adapter-admission-backpressure-and-reconnect) defines admission/backpressure/reconnect. The body-specific constraint is lifetime: once admission succeeds, queued bodies remain valid through queues, awaits, and transient reconnect. Rejected or not-yet-admitted deliveries never become Adapter-owned. Application owns buffering, retry, drop, or coalescing of new messages while disconnected.

### 2.5 Forwarding semantics

`original.Forward()` is the public instance method `Delivery Forward()`. It creates a new Delivery, leaves the original unchanged, retains the exact deliveryId/contentType and Eventa/body references, and decrements hops once. Branches derive independently: from hop 10, both A and B get 9, not 9 and 8.

Hop 0 still permits local dispatch; Forward throws InvalidOperationException. There is no TryForward or CanForward. The method only derives an envelope; Application selects forwarding, targets, Adapters, and sends. Admission/send does not consume hops or bypass controlled construction.

Submitting an existing Delivery locally is not forwarding: it neither decrements nor calls Forward implicitly. Only Application's explicit next-hop derivation consumes a hop. This separates local acceptance from TypeScript's internal pre-decremented continuation. Local consumption and relay may share the original without changing the derivation baseline.

### 2.6 Invoke protocol helper

The complete encoded Invoke body may be stored in BinaryData. It remains opaque to Core; an Invoke endpoint reads control fields using the contentType representation.

The helper sits above Core and below typed application contracts. Only semantic `protocol=invoke` enables interpretation of invokeId/isReqStream/content/error wrappers and event-ID classification into send/error/stream-end/abort/receive. With no protocol parameter it is an ordinary event.

One contentType describes the complete body. Content has no second Eventa content type; application contracts handle business conversion within the selected representation. InvokeEventType does not become a new cross-language wire requirement because protocol=invoke and event ID identify the protocol. TypeScript can keep its internal enum without C# copying its numeric values.

Invoke parses only its control structure, not business codec/schema/target policy, and does not call typed helpers implicitly. Application or application-owned endpoints make those calls.

Dynamic response-ID/body.invokeId mismatch is endpoint validation: local consumers may discard it, while generic Core/relay need not parse it.

### 2.7 Typed body codecs and helpers

Typed helpers explicitly convert application types and Delivery representations, potentially through `Encode<T>`/`Decode<T>`. Application or its typed endpoints call them at defined boundaries. Invoke and generic relay must not trigger them implicitly.

Outbound Application selects contracts from definitions or active invocations and creates contentType/body combinations for Adapters. Inbound selection follows Core deduplication/routing. Adapters do not depend on event-definition registries, Core EventContext stores no typed registry, and Core has no automatic conversion graph. One JSON helper can serve multiple JSON carriers; structured-clone and in-process Adapters retain host value domains.

## 3. JSON typed body helper

This section is a **P1 candidate Web-compatible JSON profile and design input** for an optional helper. Duplicate-property, integer, DateTime, enum, polymorphism, error-normalization, and registry rules below still require interoperability tests and business validation. They are neither Core invariants nor a frozen cross-language base protocol. They do not make JSON, ProtoJSON, or System.Text.Json a Core wire format or constrain structured-clone, in-process, or future binary Adapters.

### 3.1 Web-compatible ProtoJSON subset

#### Reference and scope

The reference is Connect RPC's JSON mode, which uses canonical ProtoJSON for browser and cross-language HTTP clients. ProtoJSON covers only Protobuf-schema types and does not guarantee unknown-field round trips. Eventa has no .proto schema; this proposal borrows descriptor-independent, Web-safe scalar conventions without claiming full ProtoJSON compatibility or adding a Protobuf runtime. [Connect Protocol](https://connectrpc.com/docs/protocol/#protocol-buffers) [ProtoJSON non-goals](https://protobuf.dev/programming-guides/json/#non-goals)

#### Base values and duplicate properties

The candidate value domain is null, boolean, string, array, object, and finite number. Encoders emit no duplicate object property names. Decoders use last-wins semantics consistent with ProtoJSON guidance and browser JSON.parse; implementations unable to preserve the required order reliably should reject duplicates. [ProtoJSON duplicate values](https://protobuf.dev/programming-guides/json/#duplicate-values)

#### Numbers and integer precision

Ordinary numbers use the shared finite IEEE 754 binary64 domain understood by Web Number. Bare numbers are restricted to JavaScript's safe-integer range `[-9007199254740991, 9007199254740991]` only when a typed contract declares integer semantics. Canonical long/ulong output is a decimal string. Typed decoders may accept safe-range bare numbers but reject out-of-range bare integers whose exactness cannot be established. [ProtoJSON int64 mapping](https://protobuf.dev/programming-guides/json/#int64)

NaN, Infinity, and -Infinity are not JSON numbers and are rejected by encoders. ProtoJSON can use special strings because its schema distinguishes strings from floats; a schemaless body cannot safely borrow that convention. Decimal, BigInteger, and JavaScript bigint are not jointly lossless Web Number values either. Contracts needing them must choose invariant decimal strings or an explicit structured representation.

#### Bytes and time

byte[] uses padded standard base64 output; decoders may accept standard or URL-safe base64 with or without padding. DateTimeOffset uses Timestamp-style UTC Z RFC 3339 output; valid offsets may decode to the same normalized instant. DateTime uses this mapping only when Kind == Utc; Local and Unspecified are rejected. [ProtoJSON type representation](https://protobuf.dev/programming-guides/json/#format-description)

#### Enums, polymorphism, and cycles

Enum wire values are explicit stable contract strings, independent of CLR backing integers. Polymorphism requires stable discriminators and complete branch metadata. Do not send assembly-qualified CLR names or copy descriptor-dependent Any/@type. Cycles are outside JSON's value domain; encoding fails rather than emitting language-specific reference markers.

These rules apply only to selected JSON typed helpers. Opaque relay must not infer CLR types because a raw number resembles an integer or a string resembles a timestamp/base64 value.

### 3.2 Missing and JSON null

Core gives no meaning to missing body versus null and adds no BodyPresence abstraction. Null is a valid runtime value whose meaning belongs to Application or an upper protocol. Adapters do not assign business semantics either. A profile promising representation-preserving round trips must avoid collapsing states that its contract distinguishes.

JSON and Invoke helpers may distinguish missing properties from explicit JSON null within their contracts. For example, missing content and content: null can have different Invoke meanings. A control parser may track property presence without making it universal Core body state.

### 3.3 Application-owned contract registry and AOT

#### Contract selection and ownership

System.Text.Json's IJsonTypeInfoResolver maps CLR Type to JSON serialization rules. An Eventa registry maps event ID or active invocation to the selected payload contract. The registry consumes resolver-generated JsonTypeInfo; it is not a resolver replacement.

Microsoft's source-generation guidance requires explicit serializable types, supports combining generated contexts with JsonTypeInfoResolver.Combine/TypeInfoResolverChain, and notes that reflection-based serialization can fail under Native AOT. [System.Text.Json source generation](https://learn.microsoft.com/dotnet/standard/serialization/system-text-json/source-generation)

Application or its typed endpoint/helper owns the registry, not Core EventContext fields, indexes, or implicit capabilities. It may share Context lifetime without transferring ownership. Core acceptance, duplicate suppression, routing, and Application-owned opaque relay depend on no registry, JsonTypeInfo, serializer options, or codec delegate.

#### Candidate registry behavior

A registry may use a private immutable/append-only index with StringComparer.Ordinal over complete event IDs. Non-generic descriptors hold closed JsonTypeInfo or encode/decode delegates. Registration validates a non-empty ID and matching CLR generic type/metadata. Re-registering the same ID/contract may be idempotent; conflicting payload types, options, or converter contracts fail immediately. Unknown IDs trigger no assembly scan, reflection fallback, or dynamic type discovery.

Dynamic response IDs must not accumulate permanently. Active invocation state already knows its response contract and releases it on completion.

#### References

SignalR is the closest protocol precedent: its parser reads target/invocation ID, then asks IInvocationBinder or active invocation state for argument/result types. Payloads carry no CLR type names. Eventa can use this message-first, contract-second sequence, but descriptors should hold AOT-safe metadata/delegates directly rather than return only Type. [SignalR Hub Protocol](https://github.com/dotnet/aspnetcore/blob/main/src/SignalR/docs/specs/HubProtocol.md) [`IInvocationBinder`](https://source.dot.net/Microsoft.AspNetCore.SignalR.Common/IInvocationBinder.cs.html)

Avalonia's generic property/event registrations bind stable names to value/event-argument types. Godot C# signals generate names and Variant conversion. Orleans separates schema identity from CLR names through generated codecs, stable member IDs, and type aliases. These support retaining `EventDefinition<TPayload>` and potentially generating catalogs and compile-time diagnostics, not copying Avalonia's global registry, Godot Variant, or Orleans' .NET-specific wire format. [Avalonia property registration](https://github.com/AvaloniaUI/Avalonia/blob/main/src/Avalonia.Base/AvaloniaProperty.cs) [Godot C# signals](https://docs.godotengine.org/en/stable/tutorials/scripting/c_sharp/c_sharp_signals.html) [Orleans serialization](https://learn.microsoft.com/dotnet/orleans/host/configuration-guide/serialization)

### 3.4 Error normalization belongs to typed helpers

TypeScript 1.0.2 has no universally applied wire error codec. JSON, structured clone, and IPC handle JavaScript Error differently. Generic Core treats error content as opaque and must not directly serialize or reconstruct CLR Exceptions.

The candidate JSON Invoke helper uses normalized remote errors with required name/message and optional stack/cause. Cause recursively uses the same shape. Name is a diagnostic category, not a stable machine code. Do not send assembly-qualified names or reflectively construct sender CLR exceptions from remote names.

Receivers use a common Eventa remote-exception/projection shape with separate remote and local stacks. Local transport, frame-decoding, and body-codec failures stay local; they must not masquerade as received remote causes.

This boundary follows mature cross-platform approaches: gRPC/Google RPC and Connect use code/message/details, JSON-RPC uses code/message/data, and Temporal separates message, stack, recursive cause, and typed failure information. None requires rebuilding the sender language's exception object. [gRPC error handling](https://grpc.io/docs/guides/error/) [`google.rpc.Status`](https://github.com/googleapis/googleapis/blob/master/google/rpc/status.proto) [Connect error](https://connectrpc.com/docs/protocol/#error-and-endstreamresponse) [JSON-RPC error object](https://www.jsonrpc.org/specification#error_object) [Temporal Failure](https://github.com/temporalio/api/blob/main/temporal/api/failure/v1/message.proto)

Valid JSON outside the normalized error shape may be retained as a raw value in a generic remote-error projection. For this projection, invalid body JSON or an unparseable control envelope is a decode failure. Stack transmission/redaction defaults, cause depth, cycles/truncation, and future stable code/details remain helper API/security choices, not Core envelope rules.

### 3.5 Forwarding unknown encoded bodies

A typed endpoint may ignore unmodeled JSON properties. Transparent relay must not decode an encoded body into an application POCO and reserialize it: that can lose unknown properties or alter numeric tokens. When Application preserves encoded representation, stable BinaryData is the forwarding source of truth; a typed projection is only a local view.

System.Text.Json can deserialize UTF-8 directly with `JsonTypeInfo<T>` without first constructing an arbitrary reflection model. WriteRawValue can re-embed a complete raw JSON value. [Microsoft `JsonSerializer.Deserialize`](https://learn.microsoft.com/dotnet/api/system.text.json.jsonserializer.deserialize?view=net-10.0) [Microsoft `WriteRawValue`](https://learn.microsoft.com/dotnet/api/system.text.json.utf8jsonwriter.writerawvalue?view=net-10.0)

A helper using JsonDocument/JsonElement must respect document lifetime and clone elements that outlive their document. This is a helper parsing-view responsibility, not a change to BinaryData-based encoded bodies. [System.Text.Json DOM](https://learn.microsoft.com/dotnet/standard/serialization/system-text-json/use-dom) [`JsonElement.Clone`](https://learn.microsoft.com/dotnet/api/system.text.json.jsonelement.clone?view=net-10.0)

Each JSON profile must state its preservation guarantee. Unknown carrier content must not be actively dismantled, normalized, or lost merely because Core/profile does not recognize it. Except for required protocol changes, untouched content should retain its representation. Raw-token implementations may provide stronger token/byte preservation, but whole-carrier byte equality is not required because forwarding changes routing state. Such profiles retain original or equivalent profile-native carrier state alongside the Delivery projection; Core has no extension storage.

### 3.6 JSON resource limits

Frame byte limits and JSON depth limits are local Adapter/codec protections, not wire fields or cross-language constants. Enforce them before allocating a complete DOM or typed payload and let hosts tighten them at trust boundaries.

Connect defines no universal message-size/depth constant; connect-go configures read/send limits per implementation. Eventa likewise leaves values to each JSON Adapter/helper rather than embedding them in the body schema. [connect-go options](https://github.com/connectrpc/connect-go/blob/main/option.go)

## 4. Acceptance criteria

### 4.1 Core and relay tests

#### Core/codec handoff

Full construction, ingress, recent-history, and listener-completion cases are in [Research 3 §7](research-3-eventa-typescript-1.0.2-compatibility.md#7-acceptance-tests). Codec handoff tests cover:

1. Unknown events route through IDeliveryTarget without registry, JsonTypeInfo, or codec delegates. Core requires no typed materialization or universal missing/null meaning. A registry may share Context lifetime while remaining externally owned.
2. Decode follows atomic recent-seen registration and routing. Failure does not roll back seen. A retained duplicate succeeds without helper calls, extra codec errors, waiting for the first operation, or inheriting its outcome. Expired/evicted entries follow the recent-history contract.
3. Targets accept only valid Delivery, not IDeliveryLike or retained state. IEventContext does not inherit IDeliveryTarget; Restore validates invariants and profile carriers retain representation outside Core.
4. Vanilla TypeScript 1.0.2 needs an explicit contentType profile. Profile tests cover representation, Media Type construction, Invoke mapping, and promised preservation.
5. Missing/different BinaryData.MediaType cannot change interpretation. Local acceptance preserves hops; Forward only derives an envelope and shares stable references without target/Adapter orchestration.

Handoff tests also preserve exactly Task AcceptAsync(Delivery delivery), with no caller token. If selected by fail-fast aggregation, a codec/endpoint exception reaches await unchanged, without a Core wrapper or aggregate-all wait. Later faults cannot alter outward failure. P0 requires neither a routing hook nor later-fault public API. Detailed cases remain in [Research 3 §7](research-3-eventa-typescript-1.0.2-compatibility.md#7-acceptance-tests).

#### Adapter/profile ownership

Verify that reusable/returned buffers never escape into Delivery and that stable BinaryData survives awaits, queues, and fan-out. Test snapshots/ownership for mutable arrays explicitly because BinaryData constructors do not copy.

For queued/reconnecting Adapters, admitted bodies remain stable through transient reconnect. TrySend false or reconnecting admission rejection must not retain the Delivery body reference.

#### Application and relay paths

Application/relay code explicitly derives original.Forward and selects next-hop targets/Adapters. Verify that unknown encoded events pass through C# nodes and that JSON relay preserves its declared JSON data-model or raw-token semantics.

Composition tests verify that profiles hand restored data to Application rather than targets and do not forward automatically. The same original may feed a local target and independently derived relay branches: local hops stay unchanged, branches decrement once, and Adapter admission/send does not decrement again. P0 composition needs no central Eventa runtime.

Bridge profiles test known non-Core properties and unknown outer/eventa properties, associating retained original/equivalent profile-native carriers with projections through re-send. Except required routing changes, untouched content must not be actively dismantled, normalized, or lost. These are profile tests, not fixed Core model tests. Body preservation alone cannot prove that a TypeScript → C# → Rust bridge retains future envelope extensions.

### 4.2 Invoke control

Test the TypeScript 1.0.2 body shapes in the new contentType envelope: seven IDs, dynamic response IDs, invokeId, isReqStream, stream-end, send-error, receive-error nesting, and abort. Only semantic protocol=invoke enables parsing. Endpoints ignore dynamic/body ID conflicts while generic relay forwards them.

Verify that C# needs neither numeric InvokeEventType values nor nested content types for Invoke content.

Invoke parses control only, chooses no business-content codec, and never invokes typed helpers implicitly. Application or its typed endpoints own business conversion.

### 4.3 JSON typed bodies

Run only when the JSON helper is selected:

1. Six JSON value categories, Unicode, finite binary64, and duplicate-property policy.
2. Typed integer safe bounds, long/ulong decimal strings, and rejection of inexact out-of-range bare integers/non-finite numbers.
3. Base64 bytes, UTC RFC 3339 timestamps, stable string enums, explicit polymorphic discriminators, and cycle rejection.
4. Missing versus explicit-null control properties where the JSON/Invoke contract defines presence semantics.
5. Unknown nested properties survive relay; typed projections do not replace the raw forwarding source.
6. Application-owned generated JsonTypeInfo round trips with reflection serialization disabled. Missing metadata/conflicting contracts fail at registration; identical descriptors register idempotently; unknown IDs trigger no discovery or Core registry storage.
7. Dynamic descriptors are released with active invocation state rather than accumulating per dynamic ID.
8. Size/depth limits apply before large allocations or typed decoding.

### 4.4 Error helpers

Cover required name/message, optional stack, nested cause, depth/cycle/truncation, separate stacks, raw preservation of valid non-normalized errors, and no reflective CLR exception construction from remote names. TypeScript helper/profile work must define sending and materialization behavior and provide bidirectional interoperability tests.

## 5. Open codec and profile interfaces

### Raw-relay public interface

Core uses object? and BinaryData for encoded representation. A public raw-relay API, if provided, must state preservation levels and how Delivery projections remain associated with retained original/equivalent profile-native carriers. The model is a projection plus the retained carrier, not a generic extras bag or Delivery.eventa.extensions. Endpoints may discard unneeded state; transparent relay keeps the association and applies only required protocol changes. The same stable GC-managed lifetime applies whether the API is public or internal.

### Physical layout of Invoke control and application content

The JSON helper retains `{ invokeId, content, isReqStream? }`. Implementations may read fields on demand from complete BinaryData or create parsed control views. Both use one Delivery contentType without nested Eventa representation metadata.

### Publishing Media Type profiles

Delivery.contentType explicitly selects representation, chosen by Application. There is no negotiation, Core converter registry, or automatic Adapter conversion. A public specification must define Eventa-owned media names and protocol parameter value syntax, case rules, defaults, and unknown-value handling.

### JSON registry and error APIs

Define whether generated resolver/context configuration belongs to an application-owned helper/endpoint or allows per-definition `JsonTypeInfo<T>` overrides, including precedence and contract identity. Registry lifetime may match Context lifetime without Core ownership. Error helpers still need remote-exception names, stack transmission/redaction defaults, cause limits, and analyzer/source-generator scope.

### Resource defaults

MaxFrameBytes, MaxBodyBytes, and MaxDepth must be finite and configurable. Their defaults are implementation/deployment policy, not Core protocol constants.

Invoke registration and multi-responder semantics are defined in [Research 3 §4.5](research-3-eventa-typescript-1.0.2-compatibility.md#45-invoke-protocol-contract). [Research 3 §4.8](research-3-eventa-typescript-1.0.2-compatibility.md#48-net-adapter-admission-backpressure-and-reconnect) defines Adapter admission, backpressure, reconnect, and lifecycle. Concrete types, exceptions, configuration, and shutdown belong to Adapter profiles, not body representation/ownership interfaces.

## 6. Relationship to Research 3

[Research 3](research-3-eventa-typescript-1.0.2-compatibility.md) defines Core/Invoke, the Delivery model, IDeliveryTarget, recent-history/dispatch completion, and Adapter admission, backpressure, reconnect, at-most-once behavior, and lifecycle.

This document builds on those contracts with body representation, stable ownership, explicit typed conversion, and raw preservation. Its JSON value-domain, registry/AOT, missing/null, and error rules are P1 candidate input. Each Adapter profile owns its value domain and implementation; JSON helper rules do not become universal Core protocol.
