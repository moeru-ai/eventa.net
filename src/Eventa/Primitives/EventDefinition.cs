namespace Eventa;

/// <summary>
/// Describes a named event and its payload shape.
/// </summary>
/// <remarks>
/// Within a single <see cref="EventContext"/>, do not reuse the same <see cref="Id"/> with a
/// different <typeparamref name="TPayload"/>. The default context treats the identifier as the
/// event's stable protocol identity for its entire lifetime.
/// </remarks>
public sealed record EventDefinition<TPayload>(string Id)
{
    public EventDefinition() : this(IdGenerator.New()) { }
}

/// <summary>
/// Describes the canonical event identifiers used by invoke-style request/response flows.
/// </summary>
/// <typeparam name="TResponse">The response payload type emitted by the receiver.</typeparam>
/// <typeparam name="TRequest">The request payload type sent by the caller.</typeparam>
/// <remarks>
/// The <see cref="Tag"/> value is the stable base identity for this invoke contract. Derived
/// event identifiers append fixed suffixes (for example <c>-send</c> and <c>-receive-error</c>)
/// so both sides can coordinate payload, completion, and error channels predictably.
/// </remarks>
public sealed record InvokeEventDefinition<TResponse, TRequest>(string Tag)
{
    public InvokeEventDefinition() : this(IdGenerator.New()) { }

    public string SendEventId => $"{Tag}-send";

    public string SendErrorId => $"{Tag}-send-error";

    public string SendStreamEndId => $"{Tag}-send-stream-end";

    public string SendAbortId => $"{Tag}-send-abort";

    public string ReceiveEventId => $"{Tag}-receive";

    public string ReceiveErrorId => $"{Tag}-receive-error";

    public string ReceiveStreamEndId => $"{Tag}-receive-stream-end";
}
