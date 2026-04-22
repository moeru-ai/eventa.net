namespace Eventa;

/// <summary>
/// Bundles the concrete protocol event definitions materialized from one
/// <see cref="InvokeEventDefinition{TResponse, TRequest}" />.
/// </summary>
/// <remarks>
/// <see cref="EventInvoke" /> and <see cref="EventStream" /> both pass this compact binding value
/// instead of rehydrating the same six event definitions at every call site.
/// </remarks>
/// <typeparam name="TResponse">The response payload type carried by receive events.</typeparam>
/// <typeparam name="TRequest">The request payload type carried by send events.</typeparam>
internal readonly record struct InvokeEventBindings<TResponse, TRequest>(
    EventDefinition<SendPayload<TRequest>> Send,
    EventDefinition<StreamEndPayload> SendStreamEnd,
    EventDefinition<AbortPayload> SendAbort,
    EventDefinition<ReceivePayload<TResponse>> Receive,
    EventDefinition<ReceiveErrorPayload> ReceiveError,
    EventDefinition<StreamEndPayload> ReceiveStreamEnd)
{
    /// <summary>
    /// Creates the concrete event bindings for the invoke or stream protocol identified by
    /// <paramref name="definition" />.
    /// </summary>
    /// <param name="definition">The logical invoke definition whose derived event ids should be bound.</param>
    public InvokeEventBindings(InvokeEventDefinition<TResponse, TRequest> definition)
        : this(
            new EventDefinition<SendPayload<TRequest>>(definition.SendEventId),
            new EventDefinition<StreamEndPayload>(definition.SendStreamEndId),
            new EventDefinition<AbortPayload>(definition.SendAbortId),
            new EventDefinition<ReceivePayload<TResponse>>(definition.ReceiveEventId),
            new EventDefinition<ReceiveErrorPayload>(definition.ReceiveErrorId),
            new EventDefinition<StreamEndPayload>(definition.ReceiveStreamEndId))
    { }
}
