namespace Eventa;

/// <summary>An immutable, transport-neutral routing envelope.</summary>
/// <remarks>
/// The content type describes the body representation. The envelope retains the original event and
/// body by reference. Applications that relay a transport message can retain its carrier separately.
/// </remarks>
public sealed record class Delivery : IDeliveryData
{
    /// <summary>The forwarding budget for a newly created delivery.</summary>
    public const int DefaultHopsRemaining = 32;

    /// <inheritdoc />
    public string contentType { get; }

    /// <inheritdoc />
    public string deliveryId { get; }

    /// <inheritdoc />
    public int hopsRemaining { get; }

    /// <inheritdoc />
    public EventMessage eventa { get; }

    private Delivery(string contentType, string deliveryId, int hopsRemaining, EventMessage eventa)
    {
        this.contentType = contentType;
        this.deliveryId = deliveryId;
        this.hopsRemaining = hopsRemaining;
        this.eventa = eventa;
    }

    /// <summary>Creates a delivery for a new event emission.</summary>
    /// <param name="contentType">A valid media type that describes the body representation.</param>
    /// <param name="eventa">The event to retain by reference, including its stable body.</param>
    /// <returns>
    /// A delivery with a generated ID, <see cref="DefaultHopsRemaining"/> hops, and the exact supplied content type.
    /// </returns>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="contentType"/> is not a valid media type.</exception>
    public static Delivery Create(string contentType, EventMessage eventa)
    {
        ArgumentNullException.ThrowIfNull(eventa);
        ArgumentNullException.ThrowIfNull(contentType);
        return !DeliveryContentType.IsValid(contentType)
            ? throw new ArgumentException("A valid media type is required.", nameof(contentType))
            : new Delivery(contentType, IdGenerator.New(), DefaultHopsRemaining, eventa);
    }

    /// <summary>Validates an existing delivery's routing fields.</summary>
    /// <param name="delivery">The structural view of the existing delivery.</param>
    /// <returns>
    /// A delivery with the same values and event reference, or the input itself if it is already a
    /// <see cref="Delivery"/>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="delivery"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// The input has a missing or invalid content type, a null or empty delivery ID, a negative hop budget,
    /// or a null event.
    /// </exception>
    public static Delivery Restore(IDeliveryData delivery)
    {
        ArgumentNullException.ThrowIfNull(delivery);
        if (delivery is Delivery valid) return valid;

        // Read each structural property once so restoration keeps exactly the values that were validated.
        string? contentType = delivery.contentType;
        string? deliveryId = delivery.deliveryId;
        int hopsRemaining = delivery.hopsRemaining;
        EventMessage? eventa = delivery.eventa;
        if (contentType is null || !DeliveryContentType.IsValid(contentType)
            || string.IsNullOrEmpty(deliveryId) || hopsRemaining < 0 || eventa is null)
        {
            throw new ArgumentException("The delivery does not satisfy Core invariants.", nameof(delivery));
        }

        return new Delivery(contentType, deliveryId, hopsRemaining, eventa);
    }

    /// <summary>Derives a new delivery for one additional hop.</summary>
    /// <returns>
    /// A delivery with one fewer hop and the same ID, content type, and event and body references.
    /// </returns>
    /// <remarks>
    /// Derive each independent branch from the original delivery. Local dispatch also works at zero hops.
    /// </remarks>
    /// <exception cref="InvalidOperationException">The forwarding budget is exhausted.</exception>
    public Delivery Forward()
    {
        return hopsRemaining == 0
            ? throw new InvalidOperationException("The delivery has no remaining forwarding hops.")
            : new Delivery(contentType, deliveryId, hopsRemaining - 1, eventa);
    }
}
