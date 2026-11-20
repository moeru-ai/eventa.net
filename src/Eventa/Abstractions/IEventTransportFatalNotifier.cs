namespace Eventa;

/// <summary>
/// Notifies invoke infrastructure that a transport has reached a terminal failure state.
/// </summary>
[Obsolete(LegacyTypedApi.TransportFatal, DiagnosticId = LegacyTypedApi.DiagnosticId)]
public interface IEventTransportFatalNotifier
{
    /// <summary>
    /// Faults pending invoke sessions with the supplied terminal transport error.
    /// </summary>
    /// <param name="error">The terminal transport error.</param>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is <see langword="null"/>.</exception>
    void NotifyTransportFatal(Exception error);
}
