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

- Covers register+emit, same handler only once, once listeners, `off(event)`, returned disposer, `off(event, handler)`, and returned disposer for a specific listener.
- Extra C# contract: `MatchExpression` subscriptions are integrated into `EventContext` dispatch and only receive matching payloads.
- Extra C# contract: adapter-aware contexts surface local and match-expression dispatches through `IEventaAdapter.OnReceived`, call `OnSent` only after local processing completes, and skip `OnSent` entirely if a local listener throws.
- Extra C# contract: the default `EventContext` fails fast if one `EventDefinition.Id` or `MatchExpression.Id` is reused with a different payload type inside the same context; first use via `On`, `Once`, or either `Emit` overload establishes that binding, `off(event)` does not release it, and only a different `EventContext` can rebind the same identifier.

### `invoke.spec.ts`

Status: `covered` + `adapted` + `deferred` + `extra`
Current C# target: `InvokeTests.cs`

- Covered: request-response, sync lazy context, request-derived error message, exact error instance propagation, abort/cancel with handler notification, concurrent invokes, same handler once, returned handler removal.
- Adapted: request-stream input and request-stream abort are currently covered at the protocol/handler layer because C# has no public client invoke overload for request streams.
- Extra C# contract within that adapted coverage: empty request streams are accepted by materializing handler state on `sendStreamEndEvent`, and pre-first-item abort still notifies the handler.
- Extra C# contract outside doc 2.2: a pre-canceled client token emits a single abort, and late cancellation after the response wins the race does not emit a redundant abort.
- Parity note: current TypeScript `invoke.ts` also materializes unknown `invokeId` values on `sendEventStreamEnd` and `sendEventAbort` for request-stream handlers.
- Deferred: async lazy context, `undefineInvokeHandler()` (specific + all handlers), batch registration, public client-side request stream invoke parity.

### `stream.spec.ts`

Status: `covered` + `extra`
Current C# target: `StreamTests.cs`

- Covered: server-streaming, `ToStreamHandler`, concurrent streams, error surfacing, abort stream, cancel stream via async enumerator disposal, abort request stream with paced input, abort request stream before first item, request stream input, `ToStreamHandler` + stream input.
- Extra C# contract: empty request streams are intentionally supported both through the public `IAsyncEnumerable<TRequest>` input path and at the protocol layer by materializing a handler when `sendEventStreamEnd` arrives before the first item, even though current TypeScript `stream.ts` ignores `sendEventStreamEnd` for unknown `invokeId`.
- Extra C# contract: in bidirectional request streams, disposing the response async enumerator or letting the handler complete the response stream early cancels the outbound request producer and prevents late request items or accidental reinvocation.
- Extra C# contract: request producers can safely register a cancellation callback after client cancellation has already happened; the late registration is invoked without throwing.
- Parity note: current TypeScript `stream.ts` still materializes state on unknown-id abort so the handler can observe cancellation.

### `invoke-shared.spec.ts`

Status: `adapted`
Current C# target: `EventaTests.cs`, `Primitives/InvokeEventDefinitionTests.cs`

- Adapted to C# by validating tag-derived event IDs and generated uniqueness because C# exposes `Tag`/`*Id` strings rather than TS `invokeType`-tagged event objects.

### `invoke-remote-methods.spec.ts`

Status: `deferred`
Current C# target: none

- No corresponding C# public API for remote method stubs.

### `context-extension-invoke-internal.spec.ts`

Status: `covered` + `adapted` + `extra`
Current C# target: `InvokeExtensionsTests.cs`

- Covered: pending invokes are rejected when the fatal event fires.
- Adapted: the TS `{ error }` payload pattern maps to C# via `RegisterAbortEvent<TPayload>(..., mapError)`, which can preserve the exact exception instance from a typed fatal-event payload.
- Extra C# contract: `RegisterAbortEvent(EventDefinition<object>)` also preserves the exact exception instance when the fatal-event payload is itself an `Exception`.
- Extra C# contract: if the mapper returns `null` or the object payload is not an `Exception`, pending invokes fault with the default `InvalidOperationException("Pending invoke aborted by fatal event.")`.

### `utils.spec.ts`

Status: `deferred`
Current C# target: none

- `isAsyncIterable()` / `isReadableStream()` have no current C# public API equivalent.
- `IdGeneratorTests.cs` is retained as separate extra baseline coverage below, but it is not a doc 2.2 `utils.spec.ts` parity target.

## Extra C# coverage

### `MatchExpressionTests.cs`

Status: `extra`

- C# design-contract coverage for `Create`, `And`, `Or`; not a direct TS spec file from doc 2.2.

### `IdGeneratorTests.cs`

Status: `extra`

- Kept as a green baseline for generated ID length, charset, and non-constant output.

### `AsyncSignalQueueTests.cs`

Status: `extra`

- Internal queue contract coverage for completion, fault, early-dispose cleanup, and ignored consumer cancellation.

## Current intent

- Keep structural tests compiling now.
- Keep `adapted` and `extra` notes explicit where C# intentionally covers protocol behavior through a different public shape or adds .NET-specific contracts.
- Preserve explicit `deferred` labels for doc scenarios that do not map to current C# public APIs.
