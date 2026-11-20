namespace Eventa;

// Keep warnings consistent on the interfaces and their concrete implementations.
internal static class LegacyTypedApi
{
    internal const string DiagnosticId = "EVENTA001";

    internal const string Envelope = "Use Eventa and Delivery for new event routing.";
    internal const string Emit = "Use EventContext.EmitAsync for new local events.";
    internal const string EmitWithOptions =
        "Use EventContext.EmitAsync for local events and pass transport options to the transport separately.";
    internal const string Subscribe =
        "Use EventContext.Subscribe(string, Func<Delivery, Task>) for new event ID subscriptions.";
    internal const string SubscribeOnce =
        "This typed overload remains available for one-shot subscriptions until a replacement is available.";
    internal const string MatchSubscription =
        "This typed overload remains available for match-expression subscriptions until a replacement is available.";
    internal const string Unsubscribe =
        "Dispose the registration returned by EventContext.Subscribe for Delivery listeners.";
    internal const string Adapter = "This API is part of the legacy typed-envelope transport.";
    internal const string Receive =
        "Have application code submit a validated Delivery through IDeliveryTarget.AcceptAsync.";
    internal const string TransportFatal =
        "This API reports transport failures to the legacy typed-envelope Invoke implementation.";
}
