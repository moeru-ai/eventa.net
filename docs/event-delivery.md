# Event delivery and forwarding

A `Delivery` carries an event, its content type, a delivery ID, and a forwarding budget.
Use it when your application needs to submit an existing event to a context, suppress
repeat submissions, or pass the same event to multiple contexts.

## Submit a delivery

Register a listener on `EventContext` with `Subscribe(string, Func<Delivery, Task>)`.
Listeners match the event ID exactly, including case. A listener receives the submitted
`Delivery` and returns a task that represents its work.

```csharp
using System;
using System.Threading.Tasks;
using Eventa;
using Event = global::Eventa.Eventa;

const string eventId = "orders:created";
using EventContext local = new();
using EventContext next = new();

using IDisposable localSubscription = local.Subscribe(eventId, delivery =>
{
    Console.WriteLine($"Local: {delivery.eventa.body}");
    return Task.CompletedTask;
});
using IDisposable nextSubscription = next.Subscribe(eventId, delivery =>
{
    Console.WriteLine($"Forwarded: {delivery.eventa.body}");
    return Task.CompletedTask;
});

// Application-defined content type for a CLR string.
Delivery original = Delivery.Create("application/vnd.example.order-text; model=clr", new Event(eventId, "Order 123"));
IDeliveryTarget target = local;
await target.AcceptAsync(original);
await target.AcceptAsync(original); // Duplicate. The local listener runs only once.
await next.AcceptAsync(original.Forward());
```

`Delivery.Create` generates a new delivery ID and starts with 32 hops. When you only
need to publish locally, `EventContext.EmitAsync(contentType, eventa)` creates and submits
a delivery for you. To submit an existing envelope, implement `IDeliveryLike` and pass
it to `Delivery.Restore`. Restore validates the routing fields and preserves their values.
Your application chooses the target and can check its cancellation token before submission.

Use the concrete `EventContext` for Delivery subscriptions and local emission, and
`IDeliveryTarget` to pass around the ability to submit existing deliveries. Delivery
dispatch selects listeners registered through the string-based `Subscribe` overload.
The current generic `IEventContext` overloads, Invoke clients, and Channels adapter
still use typed envelopes.

## Understand completion and duplicates

Each context remembers up to 10,000 delivery IDs. It records each ID before selecting
listeners, even if no listener matches or processing later fails. An entry expires five
minutes after it is recorded. Resubmitting a remembered ID succeeds immediately and
skips the listeners, even while the first submission is pending. A duplicate does not
refresh the entry's expiry. An ID can be dispatched again after it expires or is evicted
to make room for a new ID.

For a newly accepted ID, `AcceptAsync` invokes every matching listener and completes
successfully once they all finish successfully. If a listener throws or its task fails,
awaiting `AcceptAsync` propagates an original listener exception after every matching
callback has been invoked. A listener that returns `null` is treated as failing with
`InvalidOperationException`. Other started work can continue after the failure reaches
the caller.

A canceled listener task also counts as a failure. If its cancellation is the reported
failure, the task returned by `AcceptAsync` is `Faulted`, and awaiting it throws the
listener task's cancellation exception. Other listeners can continue, and a repeated
submission still succeeds while its ID is retained.

## Forward and share event data

Local acceptance preserves the hop budget. `Forward()` creates a separate delivery with
one fewer hop and the same delivery ID, content type, event, and body. Call it on the
original for each independent destination. A delivery with zero hops can still be
accepted locally. Calling `Forward()` on it throws `InvalidOperationException`.

Choose a `contentType` that describes the body representation your application uses.
Eventa validates its media type syntax and preserves the supplied string. The delivery's
`contentType` is authoritative, including when a `BinaryData` body has its own `MediaType`.
Listeners receive the body as `object?` and can decode it according to their application
contract after the delivery has been routed.

Because listeners and forwarded deliveries share the event and body references, keep the
body and backing storage stable for as long as they may be used. Snapshot temporary
storage, such as pooled or reusable receive buffers, before creating the event. If your
application relays a transport message and needs to preserve transport fields, keep its
original representation alongside the delivery and apply the hop change when forwarding it.
