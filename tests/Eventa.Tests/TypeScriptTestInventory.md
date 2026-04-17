# Eventa TS Test Inventory

Status labels:

- `covered` — current C# tests cover the doc scenario, sometimes with C#-specific adaptation
- `adapted` — current C# tests exercise the same behavior through a protocol-level or .NET-specific shape because no exact public API match exists yet
- `deferred` — the TS scenario has no current C# public API equivalent in `playground/src/Eventa`
- `extra` — C#-specific contract coverage beyond doc section 2.2

## Doc 2.2 alignment

| Doc section | Status | Current C# target | Covered / deferred details |
| --- | --- | --- | --- |
| `context.spec.ts` | `covered` | `EventContextTests.cs` | Covers register+emit, same handler only once, once listeners, `off(event)`, returned disposer, `off(event, handler)`, and returned disposer for a specific listener. |
| `invoke.spec.ts` | `covered` + `adapted` + `deferred` | `InvokeTests.cs` | Covered: request-response, sync lazy context, request-derived error message, exact error instance propagation, abort/cancel with handler notification, concurrent invokes, same handler once, returned handler removal. Adapted: request-stream input and request-stream abort are currently covered at the protocol/handler layer because C# has no public client invoke overload for request streams. Deferred: async lazy context, undefine handler, batch registration, public client-side request stream invoke parity. |
| `stream.spec.ts` | `covered` | `StreamTests.cs` | Covers server-streaming, `ToStreamHandler`, concurrent streams, error surfacing, abort stream, cancel stream via async enumerator disposal, abort request stream with paced input, request stream input, `ToStreamHandler` + stream input. |
| `invoke-shared.spec.ts` | `covered` | `EventaDefinitionTests.cs` | Adapted to C# by validating tag-derived event IDs and generated uniqueness instead of TS `invokeType` enum objects. |
| `invoke-remote-methods.spec.ts` | `deferred` | none | No corresponding C# public API for remote method stubs. |
| `context-extension-invoke-internal.spec.ts` | `covered` | `InvokeExtensionsTests.cs` | Validates `RegisterAbortEvent` through the public extension surface. |
| `utils.spec.ts` | `deferred` | `IdGeneratorTests.cs` only | `isAsyncIterable()` / `isReadableStream()` have no current C# public API equivalent. Existing `IdGenerator` coverage is retained as baseline, but it is outside doc 2.2 `utils.spec.ts` parity. |

## Extra C# coverage

| Area | Status | Notes |
| --- | --- | --- |
| `MatchExpressionTests.cs` | `extra` | C# design-contract coverage for `Create`, `And`, `Or`; not a direct TS spec file from doc 2.2. |
| `IdGeneratorTests.cs` | `extra` | Kept as a green baseline for generated ID length, charset, and non-constant output. |

## Current intent

- Keep structural tests compiling now.
- Allow behavior tests to fail until `playground/src/Eventa` implementations replace `NotImplementedException`.
- Preserve explicit `deferred` labels for doc scenarios that do not map to current C# public APIs.
