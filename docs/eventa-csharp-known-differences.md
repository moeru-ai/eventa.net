# Eventa C# Known Differences

Last reviewed: 2026-04-25

This document records behavior differences that are currently present in the
playground C# implementation under `playground/src/Eventa`, compared with the
behavior described by the TypeScript implementation comments and adjacent docs.

Scope notes:

- This is a status document for the current playground C# code, not a target-design document.
- It only lists differences that are both:
  - observable in the current implementation, and
  - not clearly called out by the current C# XML docs.
- It does not list every API-shape difference between TypeScript and C#.

## 1. Unary Invoke Client Does Not Expose Request-Stream Input

### TypeScript behavior

The TypeScript unary invoke API explicitly documents request-stream support.

- `src/invoke.ts` says unary invoke "supports unary or streaming requests, but returns a single response".
- The same comment explains that stream input is enabled by setting `Req` to `ReadableStream<T>` or `AsyncIterable<T>`.

Relevant TypeScript references:

- `src/invoke.ts:118`
- `src/invoke.ts:121`
- `src/invoke.ts:304`

### Current C# behavior

The C# unary invoke surface describes the same capability at the high level, but the public client API does not expose a way to send a request stream.

- `playground/src/Eventa/EventInvoke.cs` currently says the helpers support unary requests and request streams.
- `playground/src/Eventa/InvokeClient.cs` exposes only `InvokeAsync(TRequest request, CancellationToken cancellationToken = default)`.
- There is no overload that accepts `IAsyncEnumerable<TRequest>` or another streaming request abstraction.

Relevant C# references:

- `playground/src/Eventa/EventInvoke.cs:9`
- `playground/src/Eventa/InvokeClient.cs:33`

### Why this matters

The server side can handle request-stream unary invokes, and the test suite verifies the protocol path by emitting protocol events directly, but callers using the current public unary client API cannot actually exercise that behavior through an idiomatic client call.

Relevant test showing protocol-only coverage:

- `playground/tests/Eventa.Tests/InvokeTests.cs:296`

### Recommended documentation note

Until the client API grows a request-stream overload, the C# docs should say that request-stream unary invoke is currently supported at the protocol/handler level, but not yet exposed by `InvokeClient<TResponse, TRequest>`.

## 2. MatchExpression Does Not Have C# Equivalents for TS `once()` and Bulk `off()`

### TypeScript behavior

The TypeScript context API accepts either an event or a match expression in all three registration/removal entry points.

- `once()` accepts `Eventa<P> | EventaMatchExpression<P>`.
- `off()` accepts `Eventa<P> | EventaMatchExpression<P>` and can remove a specific handler or all handlers for that key.

Relevant TypeScript references:

- `src/context.ts:96`
- `src/context.ts:125`

The migration feasibility report also describes match-expression listener and once-listener behavior in the core context walkthrough.

Relevant docs reference:

- `playground/docs/eventa-csharp-feasibility.md:53`

### Current C# behavior

The C# context supports persistent match-expression subscriptions only.

- `IEventContext.Subscribe(MatchExpression<TPayload>, ...)` exists.
- There is no `SubscribeOnce(MatchExpression<TPayload>, ...)` overload.
- There is no `Unsubscribe(MatchExpression<TPayload>, ...)` overload for removing one or all listeners by match expression.

Relevant C# references:

- `playground/src/Eventa/Abstractions/IEventContext.cs:90`
- `playground/src/Eventa/EventContext.cs:277`

### Why this matters

This is a real behavior contraction, not just naming drift.

- TypeScript users can model one-shot pattern listeners directly.
- TypeScript users can clear all listeners attached to one match expression key.
- C# users currently need to hold on to individual `IDisposable` tokens and manually dispose them; there is no equivalent bulk removal or one-shot registration surface.

### Recommended documentation note

The C# XML docs should explicitly say that match expressions currently support persistent subscriptions only, and that one-shot or bulk-unsubscribe semantics are not yet exposed as first-class APIs.

## 3. Fatal Invoke Abort Extension Does Not Accept MatchExpression

### TypeScript behavior

The TypeScript invoke-internal extension explicitly allows either an event or a match expression to terminate pending invokes.

- `InvokeInternalConfig.abortOnEvents` is typed as `Array<Eventa<any> | EventaMatchExpression<any>>`.
- The helper comment says: "Register a fatal event/match expression that should terminate pending invokes."

Relevant TypeScript references:

- `src/context-extension-invoke-internal.ts:11`
- `src/context-extension-invoke-internal.ts:39`

### Current C# behavior

The C# invoke extension accepts only concrete `EventDefinition<TPayload>` registrations.

- `RegisterAbortEvent<TPayload>(..., EventDefinition<TPayload> fatalEvent, ...)`
- No overload accepts `MatchExpression<TPayload>`.

Relevant C# references:

- `playground/src/Eventa/InvokeExtensions.cs:34`
- `playground/src/Eventa/Support/InvokeInternalConfig.cs:15`

### Why this matters

This narrows adapter integration options.

- In TypeScript, an adapter can terminate pending invokes based on either a concrete fatal event or a pattern-based match.
- In the current C# implementation, the public extension point requires a concrete event identity and cannot express pattern-based fatal-abort rules.

### Recommended documentation note

The C# docs should state that the current abort extension supports fatal events only, not fatal match expressions.

## 4. Notes On What Is Not Listed Here

The following were intentionally not recorded as C#-specific differences:

- The `send-error` invoke event exists in both TypeScript and C# protocol definitions. Although it is not yet fully wired into the current C# invoke implementation, this is not a clean TS-vs-C# difference because the TypeScript handler side also does not currently treat it as a first-class documented server-side behavior surface.
- Private implementation differences that do not change observable behavior are out of scope for this document.

## 5. Suggested Follow-Up

If these differences should remain for now, add explicit XML remarks on the C# side at least in:

- `playground/src/Eventa/EventInvoke.cs`
- `playground/src/Eventa/Abstractions/IEventContext.cs`
- `playground/src/Eventa/InvokeExtensions.cs`

If they should not remain, the highest-impact implementation gap is the missing unary-invoke request-stream client surface.
