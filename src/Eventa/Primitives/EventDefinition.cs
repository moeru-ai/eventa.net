namespace Eventa;

/// <summary>
/// Describes a named event and its payload shape.
/// </summary>
/// <typeparam name="TPayload">The payload type carried by the event.</typeparam>
/// <param name="Id">The stable identifier used to emit and subscribe to the event.</param>
/// <remarks>
/// Within a single <see cref="EventContext"/>, do not reuse the same <see cref="Id"/> with a
/// different <typeparamref name="TPayload"/>. The default context treats the identifier as the
/// event's stable protocol identity for its entire lifetime.
/// </remarks>
public sealed record EventDefinition<TPayload>(string Id)
{
    /// <summary>
    /// Creates an event definition with a generated identifier.
    /// </summary>
    public EventDefinition() : this(IdGenerator.New()) { }
}

/// <summary>
/// Describes the canonical event identifiers used by invoke-style request/response flows.
/// </summary>
/// <param name="Tag">The stable base identifier used to derive all protocol event ids.</param>
/// <typeparam name="TResponse">The response payload type emitted by the receiver.</typeparam>
/// <typeparam name="TRequest">The request payload type sent by the caller.</typeparam>
/// <remarks>
/// The <see cref="Tag"/> value is the stable base identity for this invoke contract. Derived
/// event identifiers append fixed suffixes (for example <c>-send</c> and <c>-receive-error</c>)
/// so both sides can coordinate payload, completion, and error channels predictably.
/// </remarks>
public sealed record InvokeEventDefinition<TResponse, TRequest>(string Tag)
{
    /// <summary>
    /// Creates an invoke definition with a generated base tag.
    /// </summary>
    public InvokeEventDefinition() : this(IdGenerator.New()) { }

    /// <summary>
    /// Gets the derived event id used to send request payloads.
    /// </summary>
    public string SendEventId => $"{Tag}-send";

    /// <summary>
    /// Gets the derived event id used to surface request-side send failures.
    /// </summary>
    public string SendErrorId => $"{Tag}-send-error";

    /// <summary>
    /// Gets the derived event id used to mark the end of a request stream.
    /// </summary>
    public string SendStreamEndId => $"{Tag}-send-stream-end";

    /// <summary>
    /// Gets the derived event id used to abort an inflight invoke.
    /// </summary>
    public string SendAbortId => $"{Tag}-send-abort";

    /// <summary>
    /// Gets the derived event id used to deliver successful response payloads.
    /// </summary>
    public string ReceiveEventId => $"{Tag}-receive";

    /// <summary>
    /// Gets the derived event id used to deliver response-side failures.
    /// </summary>
    public string ReceiveErrorId => $"{Tag}-receive-error";

    /// <summary>
    /// Gets the derived event id used to mark the end of a response stream.
    /// </summary>
    public string ReceiveStreamEndId => $"{Tag}-receive-stream-end";
}
