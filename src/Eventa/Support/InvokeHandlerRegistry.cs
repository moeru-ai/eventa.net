namespace Eventa;

internal sealed class InvokeHandlerRegistry
{
    private readonly Lock _sync = new();
    private readonly Dictionary<string, Dictionary<Delegate, HandlerRegistration>> _registrations =
        new(StringComparer.Ordinal);

    public IDisposable Register(
        string eventId,
        Delegate handler,
        Func<HandlerRegistration> createRegistration)
    {
        lock (_sync)
        {
            if (!_registrations.TryGetValue(eventId, out var handlers))
            {
                handlers = [];
                _registrations[eventId] = handlers;
            }

            if (!handlers.TryGetValue(handler, out HandlerRegistration? registration))
            {
                registration = createRegistration();
                handlers[handler] = registration;
            }
        }

        return new ActionDisposable(() => Remove(eventId, handler));
    }

    private void Remove(string eventId, Delegate handler)
    {
        HandlerRegistration? registration = null;

        lock (_sync)
        {
            if (!_registrations.TryGetValue(eventId, out var handlers)
                || !handlers.TryGetValue(handler, out registration)) return;

            handlers.Remove(handler);
            if (handlers.Count == 0)
            {
                _registrations.Remove(eventId);
            }
        }

        registration.Dispose();
    }
}
