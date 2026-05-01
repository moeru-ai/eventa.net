# Eventa for C#

[![Build and Test][build-test-src]][build-test-href]
[![Run Example][run-example-src]][run-example-href]
[![License][license-src]][license-href]

Transport-agnostic, type-safe events for .NET 10, with ergonomic request/response
and streaming invoke flows built on top of event primitives.

Eventa for C# is a source-first preview of the Eventa protocol ideas in idiomatic
.NET. It uses `EventDefinition<T>` for strongly typed events, `EventContext` for
local dispatch and adapter hooks, `Task<T>` for unary invokes, `IAsyncEnumerable<T>`
for streams, `CancellationToken` for cancellation, and `IDisposable` for listener
lifetimes.

## Overview

Eventa treats events as the shared protocol boundary. You define stable event
identities once, subscribe to them through a context, and optionally compose the
same event primitives into RPC-like invoke contracts.

The C# implementation currently focuses on the core protocol layer:

- publish/subscribe event dispatch
- one-shot listeners and explicit unsubscribe support
- match-expression listeners
- unary request/response invokes
- request-stream to unary-response invokes
- server-streaming and bidirectional streaming invokes
- adapter observation hooks for send/receive activity
- fatal event and fatal match-expression hooks that abort pending invokes

## Features

- Type-safe event definitions with stable or generated ids.
- `EventContext` dispatch with direct listeners, one-shot listeners, and match
  expressions.
- .NET-native resource cleanup through `IDisposable` subscriptions.
- Unary invoke clients and handlers using `Task<T>`.
- Streaming invoke clients and handlers using `IAsyncEnumerable<T>`.
- Request-stream support for unary and streaming invoke flows.
- Cancellation through `CancellationToken`.
- AOT-oriented API shape with explicit generic payload types.
- Minimal adapter surface through `IEventaAdapter`.

## Requirements

- .NET 10 SDK
- A shell that can run `dotnet`

This repository is source-first right now. There is no NuGet installation step
documented here; clone the repository and work from the solution under this
directory.

## Getting Started

From this directory:

```sh
dotnet restore Eventa.slnx --locked-mode
dotnet build Eventa.slnx --configuration Release --no-restore
dotnet test --project tests/Eventa.Tests/Eventa.Tests.csproj --configuration Release --no-build
dotnet run --project examples/Eventa.Example/Eventa.Example.csproj --configuration Release --no-restore
```

The console example walks through basic events, cross-file event contracts,
dependency injection, unary invokes, streaming invokes, request streams,
match expressions, adapter hooks, cancellation, and fatal abort hooks.

## Event Example

```csharp
using Eventa;

using var context = new EventContext();
var moved = new EventDefinition<MovePayload>("demo:move");

using var subscription = context.Subscribe(
    moved,
    envelope => Console.WriteLine($"{envelope.Body.X},{envelope.Body.Y}"));

context.Emit(moved, new MovePayload(10, 20));

public sealed record MovePayload(int X, int Y);
```

`EventDefinition<T>` gives an event a stable protocol identity and a payload
shape. `EventContext.Subscribe` returns an `IDisposable`; disposing it removes
the listener.

## Unary Invoke Example

```csharp
using Eventa;

using var context = new EventContext();
var echo = new InvokeEventDefinition<EchoResponse, EchoRequest>("demo:rpc:echo");

using var handler = context.RegisterInvokeHandler(
    echo,
    static (EchoRequest request, CancellationToken _) =>
        Task.FromResult(new EchoResponse(request.Input.ToUpperInvariant())));

var client = context.CreateInvokeClient(echo);
var result = await client.InvokeAsync(new EchoRequest("eventa"));

Console.WriteLine(result.Output); // EVENTA

public sealed record EchoRequest(string Input);

public sealed record EchoResponse(string Output);
```

Invoke definitions derive the protocol event ids used for request payloads,
responses, errors, stream completion, and aborts. Concurrent invokes are
correlated internally so each call receives its own response or failure.

## Streaming Invoke Example

```csharp
using System.Runtime.CompilerServices;

using Eventa;

using var context = new EventContext();
var sync = new InvokeEventDefinition<SyncUpdate, SyncRequest>("demo:rpc:sync");

using var handler = context.RegisterStreamHandler(
    sync,
    SyncJob);

var client = context.CreateInvokeStreamClient(sync);

await foreach (var update in client.InvokeAsync(new SyncRequest("import", 3)))
{
    Console.WriteLine(update);
}

static async IAsyncEnumerable<SyncUpdate> SyncJob(
    SyncRequest request,
    [EnumeratorCancellation] CancellationToken cancellationToken)
{
    for (var step = 1; step <= request.Steps; step++)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await Task.Yield();
        yield return new SyncProgress(step * 100 / request.Steps);
    }

    yield return new SyncCompleted(request.JobId);
}

public sealed record SyncRequest(string JobId, int Steps);

public abstract record SyncUpdate;

public sealed record SyncProgress(int Percent) : SyncUpdate;

public sealed record SyncCompleted(string JobId) : SyncUpdate;
```

Streaming invokes return `IAsyncEnumerable<TResponse>`. Handlers can be written
as async iterators or adapted from callback-style code with
`EventStream.ToStreamHandler`.

## Project Layout

```text
.
+-- Eventa.slnx
+-- src/Eventa/                    # Core library
+-- tests/Eventa.Tests/            # xUnit v3 tests on Microsoft.Testing.Platform
+-- examples/Eventa.Example/       # Console examples
+-- docs/                          # Design and compatibility notes
```

## Development

Useful commands:

```sh
dotnet restore Eventa.slnx --locked-mode
dotnet build Eventa.slnx --configuration Release --no-restore
dotnet test --project tests/Eventa.Tests/Eventa.Tests.csproj --configuration Release --no-build
dotnet run --project examples/Eventa.Example/Eventa.Example.csproj --configuration Release --no-restore
```

The test project uses xUnit v3 with Microsoft.Testing.Platform. Package lock
files are checked in for restore reproducibility.

## TypeScript Relationship

Eventa started in TypeScript as an event-first way to express local events,
RPC, and streaming RPC across swappable transports. This C# project carries the
same core protocol idea into .NET conventions instead of mirroring the
TypeScript API shape directly.

For example, C# uses `Task<T>`, `IAsyncEnumerable<T>`, `CancellationToken`, and
`IDisposable` where the TypeScript package uses promises, readable streams,
abort signals, and unsubscribe callbacks.

## Security Note

Eventa forwards the payloads you emit. Validate data at process, network, and
trust boundaries before sending it to or accepting it from untrusted peers.

## License

MIT

[build-test-src]: https://github.com/moeru-ai/eventa.net/actions/workflows/build-test.yml/badge.svg
[build-test-href]: https://github.com/moeru-ai/eventa.net/actions/workflows/build-test.yml
[run-example-src]: https://github.com/moeru-ai/eventa.net/actions/workflows/run-example.yml/badge.svg
[run-example-href]: https://github.com/moeru-ai/eventa.net/actions/workflows/run-example.yml
[license-src]: https://img.shields.io/github/license/moeru-ai/eventa.svg?style=flat
[license-href]: https://github.com/moeru-ai/eventa/blob/main/LICENSE
