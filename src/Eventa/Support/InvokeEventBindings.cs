namespace Eventa;

/// <summary>
/// Carries one request payload for an invoke or stream call.
/// </summary>
/// <typeparam name="TRequest">The request payload type.</typeparam>
/// <param name="InvokeId">The protocol invoke id that owns the payload.</param>
/// <param name="Content">The request payload content.</param>
public sealed record SendPayload<TRequest>(string InvokeId, TRequest Content);

/// <summary>
/// Carries a request-side transport or producer error for an invoke.
/// </summary>
/// <param name="InvokeId">The protocol invoke id that observed the error.</param>
/// <param name="Error">The request-side failure.</param>
public sealed record SendErrorPayload(string InvokeId, Exception Error);

/// <summary>
/// Marks the end of a request or response stream for an invoke.
/// </summary>
/// <param name="InvokeId">The protocol invoke id whose stream has ended.</param>
public sealed record StreamEndPayload(string InvokeId);

/// <summary>
/// Carries a client- or transport-triggered abort signal for an inflight invoke.
/// </summary>
/// <param name="InvokeId">The protocol invoke id that should be aborted.</param>
/// <param name="Reason">The optional abort reason payload propagated with the signal.</param>
public sealed record AbortPayload(string InvokeId, string? Reason = null);

/// <summary>
/// Carries one successful response payload for an invoke or stream call.
/// </summary>
/// <typeparam name="TResponse">The response payload type.</typeparam>
/// <param name="InvokeId">The protocol invoke id that produced the payload.</param>
/// <param name="Content">The response payload content.</param>
public sealed record ReceivePayload<TResponse>(string InvokeId, TResponse Content);

/// <summary>
/// Carries a terminal response-side failure for an invoke.
/// </summary>
/// <param name="InvokeId">The protocol invoke id that failed.</param>
/// <param name="Error">The response-side failure.</param>
public sealed record ReceiveErrorPayload(string InvokeId, Exception Error);

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
/// <param name="Send">The event definition used to send request payloads.</param>
/// <param name="SendStreamEnd">The event definition used to mark the end of a request stream.</param>
/// <param name="SendAbort">The event definition used to abort an inflight invoke.</param>
/// <param name="Receive">The event definition used to deliver successful response payloads.</param>
/// <param name="ReceiveError">The event definition used to deliver response-side failures.</param>
/// <param name="ReceiveStreamEnd">The event definition used to mark the end of a response stream.</param>
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
