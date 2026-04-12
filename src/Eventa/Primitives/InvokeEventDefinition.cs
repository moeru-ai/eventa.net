namespace Eventa;

public sealed record InvokeEventDefinition<TResponse, TRequest>(string Tag)
{
    public InvokeEventDefinition() : this(IdGenerator.New())
    {
    }

    public string SendEventId => $"{Tag}-send";

    public string SendErrorId => $"{Tag}-send-error";

    public string SendStreamEndId => $"{Tag}-send-stream-end";

    public string SendAbortId => $"{Tag}-send-abort";

    public string ReceiveEventId => $"{Tag}-receive";

    public string ReceiveErrorId => $"{Tag}-receive-error";

    public string ReceiveStreamEndId => $"{Tag}-receive-stream-end";
}
