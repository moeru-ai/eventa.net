# Eventa for C#

[![Build and Test][build-test-src]][build-test-href]
[![Run Example][run-example-src]][run-example-href]
[![NuGet][nuget-src]][nuget-href]
[![License][license-src]][license-href]

Transport-agnostic, type-safe events for .NET 10, with ergonomic request/response
and streaming invoke flows built on top of event primitives.

Use `Delivery` to submit an event to a context, run asynchronous listeners, or
forward the event to another context. Each context suppresses recently seen delivery
IDs before invoking listeners. See the [guide to event delivery and forwarding][delivery-guide]
for examples and the relationship to the typed event APIs below.

Eventa for C# brings the Eventa protocol ideas into idiomatic .NET. It uses
`EventDefinition<T>` for strongly typed events and `EventContext` for local
dispatch. Invokes use `Task<T>` for unary results, `IAsyncEnumerable<T>` for
streams, and `CancellationToken` for cancellation. Subscriptions use
`IDisposable` for listener lifetimes.

## Overview

Eventa treats events as the shared protocol boundary. You define stable event
identities once, subscribe to them through a context, and optionally compose the
same event primitives into RPC-like invoke contracts.

Eventa supports:

- local Delivery dispatch with asynchronous listeners, duplicate suppression, and explicit forwarding
- typed publish/subscribe event dispatch
- one-shot listeners and explicit unsubscribe support
- match-expression listeners
- unary request/response invokes
- request-stream to unary-response invokes
- server-streaming and bidirectional streaming invokes
- adapter hooks for typed send and receive activity
- in-process channel adapter for connecting two contexts
- fatal event and fatal match-expression hooks that abort pending invokes

## Features

- Transport-neutral `Delivery` envelopes with explicit content types and hop budgets.
- Local admission through `IDeliveryTarget.AcceptAsync` with per-context duplicate suppression.
- Type-safe event definitions with stable or generated ids.
- Typed `EventContext` dispatch with direct listeners, one-shot listeners, and match
  expressions.
- .NET-native resource cleanup through `IDisposable` subscriptions.
- Unary invoke clients and handlers using `Task<T>`.
- Streaming invoke clients and handlers using `IAsyncEnumerable<T>`.
- Request-stream support for unary and streaming invoke flows.
- Invoke cancellation through `CancellationToken`.
- AOT-oriented API shape with explicit generic payload types.
- In-process transport for typed envelopes through `IEventaAdapter` and
  `Eventa.Adapters.Channels`.

## Requirements

- .NET 10 SDK
- A shell that can run `dotnet`

## Installation

Eventa is available on NuGet:

```sh
dotnet add package Eventa --prerelease
```

Package pages:

- <https://www.nuget.org/packages/Eventa/>

## Getting Started

After installing the package, start with the event and invoke examples below.
To run the repository examples locally:

```sh
dotnet restore Eventa.slnx --locked-mode
dotnet build Eventa.slnx --configuration Release --no-restore
dotnet test --project tests/Eventa.Tests.csproj --configuration Release --no-build
dotnet run --project examples/Eventa.Example/Eventa.Example.csproj --configuration Release --no-restore
```

The console example walks through basic events, cross-file event contracts,
dependency injection, unary invokes, streaming invokes, request streams,
match expressions, adapter hooks, cancellation, fatal abort hooks, and event
delivery and forwarding. See the [Delivery guide][delivery-guide] for a standalone
example.

## API transition

These typed-envelope APIs are marked `[Obsolete]` with compiler warning
`EVENTA001`. They remain available during migration.

- **Typed envelopes and dispatch overloads.** `IEventEnvelope`,
  `EventEnvelope<TPayload>`, and the current generic `Emit`, `Subscribe`,
  `SubscribeOnce`, and `Unsubscribe` overloads, including match-expression
  overloads. For new local routing, use `Eventa` and `Delivery` with
  `EventContext.EmitAsync` or `IDeliveryTarget.AcceptAsync`. Subscribe by
  event ID with `EventContext.Subscribe(string, Func<Delivery, Task>)`.
  Use the current typed overloads where you need one-shot or match-expression
  subscriptions today.
- **Typed transport integration.** `IEventaAdapter`, `EventContext.Adapter`,
  `IEventInboundDispatcher`, `EventContext.Receive`,
  `IEventTransportFatalNotifier`, `EventContext.NotifyTransportFatal`, and the
  typed-envelope Channels transport (`ChannelPipe`, `ChannelEndpoint`, and
  `ChannelMessage`). In the Delivery model, the transport projects its input
  into a `Delivery` and application code selects the target.

`IEventContext` remains a separate application-facing capability, and
`EventDefinition<TPayload>` continues to describe typed event contracts.
The current Invoke clients and registration helpers in `EventInvoke` and
`EventStream` still use typed envelopes. A future Invoke implementation will
use Delivery. See the [compatibility design][compatibility-design] for the
migration boundaries.

## Typed Event Example

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

## Channel Adapter Example

```csharp
using Eventa;
using Eventa.Adapters.Channels;

using var pipe = new ChannelPipe();

var echo = new InvokeEventDefinition<EchoResponse, EchoRequest>("demo:channel:echo");

using var handler = pipe.Right.RegisterInvokeHandler(
    echo,
    static (EchoRequest request, CancellationToken _) =>
        Task.FromResult(new EchoResponse(request.Input.ToUpperInvariant())));

var client = pipe.Left.CreateInvokeClient(echo);
var response = await client.InvokeAsync(new EchoRequest("eventa"));

Console.WriteLine(response.Output); // EVENTA

public sealed record EchoRequest(string Input);

public sealed record EchoResponse(string Output);
```

`ChannelPipe.Left` and `ChannelPipe.Right` are symmetric endpoints and each is
usable as an `IEventContext`. The in-process Channels adapter forwards typed
envelopes through `System.Threading.Channels` without serializing payloads.
Delivery listeners and forwarding compose directly through `EventContext` and
`IDeliveryTarget`.

Default `ChannelPipe` channels are unbounded and intended for local composition, tests,
and same-process boundaries, not as a throughput or backpressure policy. The
`Eventa` package, including the channel adapter, enables
`IsAotCompatible=true`; the channel transport path avoids reflection, dynamic
dispatch, and runtime generic construction.

## Project Layout

```text
.
+-- Eventa.slnx
+-- src/Eventa.csproj              # Eventa package project
+-- src/Eventa/                    # Core library source
+-- src/Eventa.Adapters/Channels/  # Channel adapter source
+-- tests/Eventa.Tests.csproj      # xUnit v3 tests on Microsoft.Testing.Platform
+-- tests/Eventa/                  # Core tests
+-- tests/Eventa.Adapters/Channels/ # Channel adapter tests
+-- examples/Eventa.Example/       # Console examples
+-- docs/                          # Design and compatibility notes
```

## Development

Useful commands:

```sh
dotnet restore Eventa.slnx --locked-mode
dotnet build Eventa.slnx --configuration Release --no-restore
dotnet test --project tests/Eventa.Tests.csproj --configuration Release --no-build
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

Native TypeScript 1.0.2 frames lack the `contentType` that `Delivery.Restore`
requires. Interoperability with those frames requires an explicit compatibility
profile to supply the content type and map the frame. See the
[wire compatibility design][wire-compatibility].

## Security Note

Eventa forwards the payloads you emit. Validate data at process, network, and
trust boundaries before sending it to or accepting it from untrusted peers.

## License

[MIT][license-href]

[build-test-src]: https://github.com/moeru-ai/eventa.net/actions/workflows/build-test.yml/badge.svg
[build-test-href]: https://github.com/moeru-ai/eventa.net/actions/workflows/build-test.yml
[run-example-src]: https://github.com/moeru-ai/eventa.net/actions/workflows/run-example.yml/badge.svg
[run-example-href]: https://github.com/moeru-ai/eventa.net/actions/workflows/run-example.yml
[nuget-src]: https://img.shields.io/nuget/vpre/Eventa.svg?style=flat
[nuget-href]: https://www.nuget.org/packages/Eventa/
[license-src]: https://img.shields.io/github/license/moeru-ai/eventa.net.svg?style=flat
[license-href]: https://github.com/moeru-ai/eventa.net/blob/main/LICENSE
[delivery-guide]: https://github.com/moeru-ai/eventa.net/blob/main/docs/event-delivery.md
[compatibility-design]: https://github.com/moeru-ai/eventa.net/blob/main/docs/research/research-3-eventa-typescript-1.0.2-compatibility.md
[wire-compatibility]: https://github.com/moeru-ai/eventa.net/blob/main/docs/research/research-3-eventa-typescript-1.0.2-compatibility.md#47-compatibility-layers
