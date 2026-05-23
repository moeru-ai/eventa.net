namespace Eventa;

/// <summary>
/// Notifies invoke infrastructure that a transport has reached a terminal failure state.
/// </summary>
public interface IEventTransportFatalNotifier
{
    /// <summary>
    /// Faults pending invoke sessions with the supplied terminal transport error.
    /// </summary>
    /// <param name="error">The terminal transport error.</param>
    void NotifyTransportFatal(Exception error);
}
