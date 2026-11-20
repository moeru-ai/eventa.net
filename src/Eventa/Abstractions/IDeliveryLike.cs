namespace Eventa;

/// <summary>A structural view to validate with <see cref="Delivery.Restore"/> before ingress.</summary>
/// <remarks>
/// Implement this interface to project the routing fields from an existing envelope. Ensure the
/// projected event and its body use stable storage before submitting the restored delivery.
/// </remarks>
public interface IDeliveryLike
{
    /// <summary>Gets the authoritative representation media type, if supplied.</summary>
    string? contentType { get; }

    /// <summary>Gets the logical delivery identity, if supplied.</summary>
    string? deliveryId { get; }

    /// <summary>Gets the remaining forwarding budget.</summary>
    int hopsRemaining { get; }

    /// <summary>Gets the event projection, if supplied.</summary>
    Eventa? eventa { get; }
}
