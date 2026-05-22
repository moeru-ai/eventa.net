using Eventa;

namespace Eventa.Adapters.Channels;

/// <summary>
/// Carries one Eventa envelope and optional adapter metadata across an in-process channel.
/// </summary>
/// <param name="Envelope">The already-created Eventa envelope.</param>
/// <param name="Options">Optional adapter metadata associated with the envelope.</param>
public sealed record ChannelMessage(
    IEventEnvelope Envelope,
    object? Options = null);

/// <summary>
/// Payload emitted when a channel endpoint observes terminal transport closure.
/// </summary>
/// <param name="Error">The terminal transport error.</param>
public sealed record ChannelClosedPayload(Exception? Error);

/// <summary>
/// Defines built-in channel adapter events.
/// </summary>
public static class ChannelEvents
{
    /// <summary>
    /// Gets the event emitted locally when a channel endpoint closes.
    /// </summary>
    public static EventDefinition<ChannelClosedPayload> Closed { get; } =
        new("eventa:channels:closed");
}

/// <summary>
/// Thrown when a channel endpoint has reached a terminal closed state.
/// </summary>
public sealed class ChannelClosedException : Exception
{
    /// <summary>
    /// Creates a channel-closed exception with a message.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public ChannelClosedException(string message) : base(message) { }

    /// <summary>
    /// Creates a channel-closed exception with a message and underlying cause.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The underlying terminal cause.</param>
    public ChannelClosedException(string message, Exception innerException) : base(message, innerException) { }
}
