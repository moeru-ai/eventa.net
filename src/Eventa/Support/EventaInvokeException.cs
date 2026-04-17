namespace Eventa;

public sealed class EventaInvokeException : Exception
{
    public EventaInvokeException()
    {
    }

    public EventaInvokeException(string message)
        : base(message)
    {
    }

    public EventaInvokeException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
