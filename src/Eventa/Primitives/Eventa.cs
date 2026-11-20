namespace Eventa;

/// <summary>An event identity and opaque, stable application body.</summary>
/// <remarks>
/// The caller is responsible for keeping the body and its backing storage stable while any delivery
/// references them. Snapshot mutable or transient storage before retaining it. Local listeners and
/// forwarded deliveries share the same references.
/// </remarks>
public sealed class Eventa
{
    /// <summary>Gets the event identifier. Empty and whitespace identifiers are valid.</summary>
    public string id { get; }

    /// <summary>Gets the opaque body. Core assigns no business meaning to null.</summary>
    public object? body { get; }

    /// <summary>Creates an event without interpreting or copying its body.</summary>
    /// <param name="id">
    /// The exact identifier used to select local listeners. Empty and whitespace values are valid.
    /// </param>
    /// <param name="body">The stable application value to retain, or <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="id"/> is <see langword="null"/>.</exception>
    public Eventa(string id, object? body = null)
    {
        ArgumentNullException.ThrowIfNull(id);
        this.id = id;
        this.body = body;
    }
}
