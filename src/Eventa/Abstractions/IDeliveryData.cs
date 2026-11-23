namespace Eventa;

/// <summary>Provides the fields used to restore a delivery.</summary>
/// <remarks>
/// Implement this interface to project the routing fields from an existing envelope. Nullable
/// fields can be absent. <see cref="Delivery.Restore"/> validates them before ingress. The
/// projected event and its body must use stable storage.
/// </remarks>
public interface IDeliveryData
{
    /// <summary>Gets the authoritative representation media type, if supplied.</summary>
    string? contentType { get; }

    /// <summary>Gets the logical delivery identity, if supplied.</summary>
    string? deliveryId { get; }

    /// <summary>Gets the remaining forwarding budget.</summary>
    int hopsRemaining { get; }

    /// <summary>Gets the event projection, if supplied.</summary>
    EventMessage? eventa { get; }
}
