# Eventa TS Test Inventory

Status labels:

- `covered` — current C# tests cover the doc scenario, sometimes with C#-specific adaptation
- `adapted` — current C# tests exercise the same behavior through a protocol-level or .NET-specific shape because no exact public API match exists yet
- `deferred` — the TS scenario has no current C# public API equivalent in `playground/src/Eventa`
- `extra` — C#-specific contract coverage beyond doc section 2.2

## Doc 2.2 alignment

### `context.spec.ts`

Status: `covered` + `extra`
Current C# target: `EventContextTests.cs`

- Covers `Subscribe`+`Emit`, same handler only once, `SubscribeOnce` listeners, `Unsubscribe(event)`, returned disposer, `Unsubscribe(event, handler)`, returned disposer for a specific listener, and match-expression `SubscribeOnce` / `Unsubscribe(matchExpression)` / `Unsubscribe(matchExpression, handler)` semantics.
- Extra C# contract: `MatchExpression` subscriptions are integrated into `EventContext` dispatch, only receive matching payloads, and support one-shot, bulk-unsubscribe, and handler-specific unsubscribe behavior.
- Extra C# contract: adapter-aware contexts surface local and match-expression dispatches through `IEventaAdapter.OnReceived` using `EventEnvelope<TPayload>` values, preserve the original event id in `envelope.EventId` even when `eventId` is a match-expression id, call `OnSent` only after local processing completes, and skip `OnSent` entirely if a local listener throws.
- Extra C# contract: the default `EventContext` fails fast if one `EventDefinition.Id` or `MatchExpression.Id` is reused with a different payload type inside the same context; first use via `Subscribe`, `SubscribeOnce`, or either `Emit` overload establishes that binding, `Unsubscribe(event)` and `Unsubscribe(matchExpression)` do not release it, and only a different `EventContext` can rebind the same identifier.

### `invoke.spec.ts`

Status: `covered` + `deferred` + `extra`
Current C# target: `InvokeTests.cs`

- Covered: request-response, sync lazy context, unary invoke request-stream input, request-derived error message, exact error instance propagation, abort/cancel with handler notification, concurrent invokes, same handler once, returned handler removal.
- Extra C# contract: unary request-stream invokes accept empty request streams, pre-canceled request streams do not enumerate the outbound source and emit a single abort, and handler failures raised while consuming streamed requests propagate with the exact exception instance.
- Extra C# contract outside doc 2.2: a pre-canceled unary client token emits a single abort without sending the request, fatal-event completion can also win before request emit so the invoke faults without sending the request, and late cancellation after the response wins the race does not emit a redundant abort.
- Parity note: current TypeScript `invoke.ts` also materializes unknown `invokeId` values on `sendEventStreamEnd` and `sendEventAbort` for request-stream handlers.
- Deferred: async lazy context, `undefineInvokeHandler()` (specific + all handlers), batch registration.

### `stream.spec.ts`

Status: `covered` + `extra`
Current C# target: `StreamTests.cs`

- Covered: server-streaming, sync lazy context, `ToStreamHandler`, concurrent streams, error surfacing, abort stream, cancel stream via async enumerator disposal, abort request stream with paced input, abort request stream before first item, request stream input, `ToStreamHandler` + stream input.
- Extra C# contract: empty request streams are intentionally supported both through the public `IAsyncEnumerable<TRequest>` input path and at the protocol layer by materializing a handler when `sendEventStreamEnd` arrives before the first item, even though current TypeScript `stream.ts` ignores `sendEventStreamEnd` for unknown `invokeId`.
- Extra C# contract: in bidirectional request streams, disposing the response async enumerator or letting the handler complete the response stream early cancels the outbound request producer and prevents late request items or accidental reinvocation.
- Extra C# contract: aborting a request stream before the first item is surfaced to the handler as cancellation rather than a protocol receive-error path; the handler sees a canceled token and no `receiveError` event is emitted.
- Extra C# contract: a pre-canceled client token emits a single abort without sending the unary request payload, and a pre-canceled request-stream invoke does not enumerate the outbound request source at all.
- Extra C# contract: a unary stream invoke that is disposed before its queued send could run does not leave a late-starting handler behind; once client cleanup wins, the initial unary send must not be deferred past that cleanup boundary.
- Extra C# contract: a request-stream invoke that is disposed before its queued send starts does not enumerate the outbound request source, does not emit any request item, and emits only the client abort.
- Extra C# contract: request producers can safely register a cancellation callback after client cancellation has already happened; the late registration is invoked without throwing.
- Parity note: current TypeScript `stream.ts` still materializes state on unknown-id abort so the handler can observe cancellation.

### `invoke-shared.spec.ts`

Status: `adapted`
Current C# target: `DefinitionConstructionTests.cs`, `Primitives/InvokeEventDefinitionTests.cs`

- Adapted to C# by validating tag-derived event IDs and generated uniqueness because C# exposes `Tag`/`*Id` strings rather than TS `invokeType`-tagged event objects.

### `invoke-remote-methods.spec.ts`

Status: `deferred`
Current C# target: none

- No corresponding C# public API for remote method stubs.

### `context-extension-invoke-internal.spec.ts`

Status: `covered` + `adapted` + `extra`
Current C# target: `InvokeExtensionsTests.cs`, `InvokeTests.cs`

- Covered: pending invokes are rejected when the fatal event or registered fatal match expression fires, including the case where the fatal event completes the invoke before the client request is emitted.
- Extra C# contract: fatal-event registrations only reject pending unary invokes; `CreateInvokeStreamClient()` sessions continue to stream normally when the same fatal event fires mid-stream.
- Adapted: the TS `{ error }` payload pattern maps to C# via `RegisterAbortEvent<TPayload>(..., mapError)`, which can preserve the exact exception instance from a typed fatal-event payload.
- Extra C# contract: if the fatal event fires reentrantly while its subscription is still being attached, the pending invoke still disposes that fatal-event subscription exactly once.
- Extra C# contract: `RegisterAbortEvent(EventDefinition<object>)` also preserves the exact exception instance when the fatal-event payload is itself an `Exception`.
- Extra C# contract: if the mapper returns `null` or the object payload is not an `Exception`, pending invokes fault with the default `InvalidOperationException("Pending invoke aborted by fatal event.")`.

### `utils.spec.ts`

Status: `deferred`
Current C# target: none

- `isAsyncIterable()` / `isReadableStream()` have no current C# public API equivalent.
- `IdGeneratorTests.cs` is retained as separate extra baseline coverage below, but it is not a doc 2.2 `utils.spec.ts` parity target.

## Extra C# coverage

### `IdiomaticApiTests.cs`

Status: `extra`

- Public C# API smoke coverage for constructor-created definitions and context-first extension methods introduced by the idiomatic API refactor: `CreateInvokeClient`/`RegisterInvokeHandler`, `CreateInvokeStreamClient`/`RegisterStreamHandler`, and `SubscribeOnce`/`Subscribe`/`Unsubscribe`.

### `MatchExpressionTests.cs`

Status: `extra`

- C# design-contract coverage for `Create`, `And`, `Or`; not a direct TS spec file from doc 2.2.

### `IdGeneratorTests.cs`

Status: `extra`

- Kept as a green baseline for generated ID length, charset, and non-constant output.

### `AsyncSignalQueueTests.cs`

Status: `extra`

- Internal queue contract coverage for completion, fault, early-dispose cleanup, ignored consumer cancellation, and concurrent completion that must not drop already-accepted writes.

### `RunOnceActionTests.cs`

Status: `extra`

- Internal helper coverage for at-most-once callback execution, used by disposal and client-cancellation paths that must not run the same cleanup twice.

### `DeferredDisposableTests.cs`

Status: `extra`

- Internal helper coverage for placeholder disposables that may be disposed before the real subscription or cancellation registration is attached; also asserts duplicate attachment is rejected without leaking the original cleanup handle.

### `InvokeSessionEngineTests.cs`

Status: `extra`

- Internal client-session coverage for the shared invoke lifecycle extraction: inline and queued send faults on both unary and stream invokes fault locally without emitting `SendAbort`, keeping protocol aborts reserved for client-initiated cancellation.

### `RequestStreamInvocationTrackerTests.cs`

Status: `extra`

- Internal tracker coverage for publishing request-stream state before handler startup, starting execution outside the tracker lock, and rolling back broken inflight state when startup throws synchronously.

### `RequestStreamInvocationStateTests.cs`

Status: `extra`

- Internal request-stream state coverage for abort semantics: abort cancels the handler token and faults pending request readers with an `OperationCanceledException` tied to that same token.

## Current intent

- Keep structural tests compiling now.
- Keep `adapted` and `extra` notes explicit where C# intentionally covers protocol behavior through a different public shape or adds .NET-specific contracts.
- Preserve explicit `deferred` labels for doc scenarios that do not map to current C# public APIs.
