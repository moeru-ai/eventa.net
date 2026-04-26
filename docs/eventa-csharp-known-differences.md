# Eventa C# Known Differences

Last reviewed: 2026-04-26

This document records behavior differences that are currently present in the
playground C# implementation under `playground/src/Eventa`, compared with the
behavior described by the TypeScript implementation comments and adjacent docs.

Current status: the differences previously recorded in this file were resolved
in the C# playground implementation. The sections below are kept as a compact
resolution log.

Scope notes:

- This is a status document for the current playground C# code, not a target-design document.
- It only lists differences that are both:
  - observable in the current implementation, and
  - not clearly called out by the current C# XML docs.
- It does not list every API-shape difference between TypeScript and C#.

## 1. Unary Invoke Client Request-Stream Input

Status: resolved.

The TypeScript unary invoke API explicitly documents request-stream support:

- `src/invoke.ts` says unary invoke "supports unary or streaming requests, but returns a single response".
- The same comment explains that stream input is enabled by setting `Req` to `ReadableStream<T>` or `AsyncIterable<T>`.

Relevant TypeScript references:

- `src/invoke.ts:118`
- `src/invoke.ts:121`
- `src/invoke.ts:304`

### C# behavior

The C# unary invoke client now exposes both unary and request-stream input:

- `InvokeAsync(TRequest request, CancellationToken cancellationToken = default)`
- `InvokeAsync(IAsyncEnumerable<TRequest> request, CancellationToken cancellationToken = default)`

The request-stream overload emits request items through the existing invoke
protocol, emits request stream end on normal completion, and aborts without
enumerating the producer when the caller token is already canceled.

Relevant C# references:

- `playground/src/Eventa/InvokeClient.cs`
- `playground/tests/Eventa.Tests/InvokeTests.cs`

## 2. MatchExpression Equivalents for TS `once()` and Bulk `off()`

Status: resolved.

The TypeScript context API accepts either an event or a match expression in all
three registration/removal entry points.

- `once()` accepts `Eventa<P> | EventaMatchExpression<P>`.
- `off()` accepts `Eventa<P> | EventaMatchExpression<P>` and can remove a specific handler or all handlers for that key.

Relevant TypeScript references:

- `src/context.ts:96`
- `src/context.ts:125`

The migration feasibility report also describes match-expression listener and once-listener behavior in the core context walkthrough.

Relevant docs reference:

- `playground/docs/eventa-csharp-feasibility.md:53`

### C# behavior

The C# context now exposes match-expression equivalents:

- `IEventContext.Subscribe(MatchExpression<TPayload>, ...)` exists.
- `IEventContext.SubscribeOnce(MatchExpression<TPayload>, ...)` exists.
- `IEventContext.Unsubscribe(MatchExpression<TPayload>, ...)` exists and can remove one handler or all regular/one-shot listeners for the match expression.

Relevant C# references:

- `playground/src/Eventa/Abstractions/IEventContext.cs`
- `playground/src/Eventa/EventContext.cs`
- `playground/tests/Eventa.Tests/EventContextTests.cs`

## 3. Fatal Invoke Abort Extension Does Not Accept MatchExpression

Status: resolved.

The TypeScript invoke-internal extension explicitly allows either an event or a match expression to terminate pending invokes.

- `InvokeInternalConfig.abortOnEvents` is typed as `Array<Eventa<any> | EventaMatchExpression<any>>`.
- The helper comment says: "Register a fatal event/match expression that should terminate pending invokes."

Relevant TypeScript references:

- `src/context-extension-invoke-internal.ts:11`
- `src/context-extension-invoke-internal.ts:39`

### C# behavior

The C# invoke extension now accepts concrete events and match expressions:

- `RegisterAbortEvent<TPayload>(..., EventDefinition<TPayload> fatalEvent, ...)`
- `RegisterAbortEvent<TPayload>(..., MatchExpression<TPayload> fatalMatch, ...)`

The internal abort registration still stores a typed subscribe callback so the
pending invoke path remains AOT-safe and does not reflect over payload shapes.

Relevant C# references:

- `playground/src/Eventa/InvokeExtensions.cs`
- `playground/src/Eventa/Support/InvokeInternalConfig.cs`
- `playground/tests/Eventa.Tests/InvokeExtensionsTests.cs`

## 4. Notes On What Is Not Listed Here

The following were intentionally not recorded as C#-specific differences:

- The `send-error` invoke event exists in both TypeScript and C# protocol definitions. Although it is not yet fully wired into the current C# invoke implementation, this is not a clean TS-vs-C# difference because the TypeScript handler side also does not currently treat it as a first-class documented server-side behavior surface.
- Private implementation differences that do not change observable behavior are out of scope for this document.

## 5. Suggested Follow-Up

The `send-error` request-side protocol event is still not listed as a C#-specific
difference for the reason above. If it becomes a documented first-class behavior
surface in TypeScript or C#, track it separately from the resolved gaps in this
file.
