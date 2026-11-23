using System.Reflection;

namespace Eventa.Tests;

public sealed class DeliveryTests
{
    [Theory]
    [InlineData("")]
    [InlineData(" \t")]
    [InlineData("orders:created")]
    public void EventMessage_WithNonNullId_PreservesIdAndOpaqueBody(string id)
    {
        object body = new();
        EventMessage eventa = new(id, body);
        Assert.Same(id, eventa.id);
        Assert.Same(body, eventa.body);
        Assert.Null(new EventMessage(id).body);
        Assert.NotEqual(eventa, new EventMessage(id, body));
        Assert.Throws<ArgumentNullException>(() => new EventMessage(null!));
    }

    [Fact]
    public void PublicModel_WhenInspected_ExposesOnlyGetOnlyProtocolProperties()
    {
        Assert.True(typeof(EventMessage).IsSealed);
        Assert.True(typeof(Delivery).IsSealed);
        Assert.Empty(typeof(Delivery).GetConstructors());
        Assert.Equal(["body", "id"], ProtocolProperties(typeof(EventMessage)));
        Assert.Equal(["contentType", "deliveryId", "eventa", "hopsRemaining"], ProtocolProperties(typeof(Delivery)));
        Assert.All(typeof(Delivery).GetProperties(), property => Assert.Null(property.SetMethod));
        Assert.All(typeof(EventMessage).GetProperties(), property => Assert.Null(property.SetMethod));
        Assert.Null(typeof(EventMessage).GetMethod("<Clone>$"));
        Assert.True(typeof(IDeliveryData).IsAssignableFrom(typeof(Delivery)));
    }

    [Fact]
    public void Create_WithValidInputs_GeneratesIdAndPreservesDefaultsAndReferences()
    {
        const string contentType = "Application/Example; model=CLR";
        object body = new();
        EventMessage eventa = new("", body);
        Delivery delivery = Delivery.Create(contentType, eventa);
        Assert.False(string.IsNullOrEmpty(delivery.deliveryId));
        Assert.NotEqual(delivery.deliveryId, Delivery.Create(contentType, eventa).deliveryId);
        Assert.Equal(32, Delivery.DefaultHopsRemaining);
        Assert.Equal(Delivery.DefaultHopsRemaining, delivery.hopsRemaining);
        Assert.Same(contentType, delivery.contentType);
        Assert.Same(eventa, delivery.eventa);
        Assert.Same(body, delivery.eventa.body);
        Assert.Throws<ArgumentNullException>(() => Delivery.Create(contentType, null!));
        Assert.Throws<ArgumentNullException>(() => Delivery.Create(null!, eventa));
    }

    [Theory]
    [InlineData("", 0)]
    [InlineData(" \t", 1)]
    [InlineData("orders:created", int.MaxValue)]
    public void Restore_WithValidInput_PreservesFieldsAndExistingDelivery(string eventId, int hops)
    {
        const string contentType = "Application/Example; FORMAT=\"original\";model=clr";
        const string id = " \t";
        EventMessage eventa = new(eventId, new object());
        Delivery delivery = Delivery.Restore(new DeliveryInput(contentType, id, hops, eventa));
        Assert.Same(contentType, delivery.contentType);
        Assert.Same(id, delivery.deliveryId);
        Assert.Equal(hops, delivery.hopsRemaining);
        Assert.Same(eventa, delivery.eventa);
        Assert.Same(delivery, Delivery.Restore(delivery));
        Delivery copy = delivery with { };
        Assert.NotSame(copy, delivery);
        Assert.Equal(delivery, copy);
        Assert.Same(delivery.eventa, copy.eventa);
        Assert.Same(delivery.eventa.body, copy.eventa.body);
        Assert.Throws<ArgumentNullException>(() => Delivery.Restore(null!));
    }

    [Fact]
    public void Restore_WithInvalidMembers_RejectsWithoutRepair()
    {
        EventMessage eventa = new("event");
        DeliveryInput valid = new("application/example", "id", 2, eventa);
        DeliveryInput[] invalid =
        [
            valid with { contentType = null },
            valid with { contentType = "" },
            valid with { contentType = "not a media type" },
            valid with { deliveryId = null },
            valid with { deliveryId = "" },
            valid with { hopsRemaining = -1 },
            valid with { eventa = null }
        ];
        foreach (DeliveryInput input in invalid)
        {
            Assert.Throws<ArgumentException>(() => Delivery.Restore(input));
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(int.MaxValue)]
    public void Forward_WithPositiveHopBudget_DerivesIndependentBranchesAndPreservesReferences(int hops)
    {
        Delivery original = Delivery.Restore(new DeliveryInput("Application/Example;x=\"X\"", "id", hops,
            new EventMessage("event", new object())));
        Delivery left = original.Forward();
        Delivery right = original.Forward();
        Assert.NotSame(original, left);
        Assert.NotSame(left, right);
        Assert.Equal(hops, original.hopsRemaining);
        Assert.Equal(hops - 1, left.hopsRemaining);
        Assert.Equal(hops - 1, right.hopsRemaining);
        Assert.Same(original.contentType, left.contentType);
        Assert.Same(original.deliveryId, left.deliveryId);
        Assert.Same(original.eventa, left.eventa);
        Assert.Same(original.eventa.body, left.eventa.body);
        if (hops > 1)
        {
            Assert.Equal(hops - 2, left.Forward().hopsRemaining);
        }
        else
        {
            Assert.Throws<InvalidOperationException>(left.Forward);
        }
    }

    private static string[] ProtocolProperties(Type type)
    {
        return [.. type.GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Select(property => property.Name).Order(StringComparer.Ordinal)];
    }
}
