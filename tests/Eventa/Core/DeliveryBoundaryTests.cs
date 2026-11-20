using System.Buffers;
using System.Globalization;
using System.Text;

namespace Eventa.Tests;

public sealed class DeliveryBoundaryTests
{
    [Fact]
    public async Task OpaqueRouting_WithRuntimeBinaryAndNullBodies_PreservesOpaqueReferences()
    {
        using EventContext context = new();
        object?[] bodies = [new UninspectableBody(), new BinaryData([0xff, 0, 0x81]), null];
        List<Delivery> received = [];
        using IDisposable registration = context.Subscribe("unknown", item =>
        {
            received.Add(item);
            return Task.CompletedTask;
        });

        foreach (object? body in bodies)
        {
            Delivery delivery = Delivery.Create("application/unknown", new Eventa("unknown", body));
            await context.AcceptAsync(delivery);
            Assert.Same(body, received[^1].eventa.body);
            Assert.Same(delivery, received[^1]);
        }

        Assert.Equal(3, received.Count);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("text/plain")]
    [InlineData("not a valid protocol media type")]
    public async Task BinaryDataMetadata_WithAnyMediaType_LeavesDeliveryContentTypeAuthoritative(string? mediaType)
    {
        using EventContext context = new();
        BinaryData body = new([1, 2, 3], mediaType);
        const string authoritative = "Application/Unknown; MODEL=\"opaque\"";
        Eventa eventa = new("unknown", body);
        Delivery created = Delivery.Create(authoritative, eventa);
        Delivery restored = Delivery.Restore(new DeliveryInput(authoritative, "remote-id", 0, eventa));
        int calls = 0;
        using IDisposable registration = context.Subscribe("unknown", item =>
        {
            calls++;
            Assert.Same(body, item.eventa.body);
            Assert.Same(authoritative, item.contentType);
            return Task.CompletedTask;
        });
        await context.AcceptAsync(created);
        await context.AcceptAsync(restored);
        Assert.Equal(2, calls);
        Assert.Throws<ArgumentException>(() => Delivery.Restore(new DeliveryInput(null, "id", 0, eventa)));
        Assert.Throws<ArgumentException>(() => Delivery.Create("invalid", eventa));
    }

    [Fact]
    public async Task EndpointDecode_WithFailedDeliveryResubmitted_RunsOnlyOnce()
    {
        using EventContext context = new();
        Delivery unknown = Delivery.Create("application/unknown", new Eventa("unknown", new UninspectableBody()));
        Delivery selected = Delivery.Create("application/unknown", new Eventa("selected", unknown.eventa.body));
        int decodes = 0;
        FormatException error = new("application-owned decode failure");
        using IDisposable registration = context.Subscribe("selected", item =>
        {
            Assert.Same(selected, item);
            decodes++;
            throw error;
        });
        await context.AcceptAsync(unknown);
        Assert.Equal(0, decodes);
        Assert.Same(error, await Assert.ThrowsAsync<FormatException>(() => context.AcceptAsync(selected)));
        await context.AcceptAsync(selected);
        Assert.Equal(1, decodes);
    }

    [Fact]
    public async Task StableOwnership_WithSnapshotOfPooledMemory_SurvivesReuseAwaitAndFanOut()
    {
        byte[] pooled = ArrayPool<byte>.Shared.Rent(3);
        pooled[0] = 1;
        pooled[1] = 2;
        pooled[2] = 3;
        // BinaryData wraps memory; the ingress owner explicitly snapshots before returning it.
        BinaryData transient = new(pooled.AsMemory(0, 3));
        BinaryData stable = new(transient.ToArray());
        Delivery original = Delivery.Restore(new DeliveryInput("application/octet-stream", "stable", 2,
            new Eventa("event", stable)));
        Array.Fill<byte>(pooled, 0xee);
        ArrayPool<byte>.Shared.Return(pooled);

        using EventContext local = new();
        using EventContext remote = new();
        TaskCompletionSource resume = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<Delivery> retained = [];
        using IDisposable first = local.Subscribe("event", async item =>
        {
            retained.Add(item);
            await resume.Task;
            Assert.Equal(new byte[] { 1, 2, 3 }, ((BinaryData)item.eventa.body!).ToArray());
        });
        using IDisposable second = local.Subscribe("event", item =>
        {
            retained.Add(item);
            return Task.CompletedTask;
        });
        using IDisposable third = remote.Subscribe("event", item =>
        {
            retained.Add(item);
            return Task.CompletedTask;
        });

        Delivery branch = original.Forward();
        Task localAcceptance = local.AcceptAsync(original);
        await remote.AcceptAsync(branch);
        Assert.False(localAcceptance.IsCompleted);
        Assert.Equal(3, retained.Count);
        Assert.Same(original, retained[0]);
        Assert.Same(original, retained[1]);
        Assert.Same(branch, retained[2]);
        Assert.All(retained, item => Assert.Same(original.eventa, item.eventa));
        Assert.All(retained, item => Assert.Same(stable, item.eventa.body));
        resume.SetResult();
        await localAcceptance.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(new byte[] { 1, 2, 3 }, stable.ToArray());
    }

    [Fact]
    public async Task Application_WithExplicitRemoteConversion_PreservesLocalRuntimeBody()
    {
        using EventContext local = new();
        using EventContext remote = new();
        RuntimeValue value = new(42);
        Delivery original = Delivery.Create("application/example-runtime", new Eventa("event", value));
        using IDisposable a = local.Subscribe("event", item =>
        {
            Assert.Same(value, item.eventa.body);
            return Task.CompletedTask;
        });
        using IDisposable b = remote.Subscribe("event", item =>
        {
            Assert.Equal("application/example-binary", item.contentType);
            Assert.Equal("*"u8.ToArray(), Assert.IsType<BinaryData>(item.eventa.body).ToArray());
            return Task.CompletedTask;
        });

        // This is user-owned conversion, not a Core registry or an Adapter capability graph.
        Delivery next = original.Forward();
        Delivery converted = Delivery.Restore(new DeliveryInput("application/example-binary", next.deliveryId,
            next.hopsRemaining, new Eventa(next.eventa.id, new BinaryData([(byte)value.Number]))));
        await Task.WhenAll(local.AcceptAsync(original), remote.AcceptAsync(converted));
        Assert.Same(value, original.eventa.body);
        Assert.Equal(original.deliveryId, converted.deliveryId);
        Assert.Equal(32, original.hopsRemaining);
        Assert.Equal(31, converted.hopsRemaining);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RetainedCarrier_WithLocalDispatchAndForwarding_PreservesUntouchedRepresentation(bool localFirst)
    {
        // A test-only profile seam. These raw spans stand for an already validated, stable carrier;
        // there is no production wire decoder, retained-carrier API, or generic extras dictionary.
        const string prefix = "{ \"contentType\":\"application/example\", \"deliveryId\":\"remote\","
            + " \"hopsRemaining\":";
        const string suffix = " , \"unknownOuter\":1.2300e+07, \"eventa\": { \"id\":\"event\","
            + " \"type\":\"event\", \"metadata\": {\"text\":\"\\u0041\"},"
            + " \"invokeMetadata\": {\"future\":true}, \"unknownEventa\": [ 1,  2 ], \"body\":null } }";
        TestCarrier retained = new(Encoding.UTF8.GetBytes(prefix), Encoding.UTF8.GetBytes(suffix));
        DeliveryInput projection = new("application/example", "remote", 1, new Eventa("event"));
        RetainedProjection inbound = new(Delivery.Restore(projection), retained);
        using EventContext context = new();
        using IDisposable local = context.Subscribe("event", item =>
        {
            Assert.Same(inbound.Delivery, item);
            return Task.CompletedTask;
        });

        if (localFirst)
        {
            await context.AcceptAsync(inbound.Delivery);
        }

        RetainedProjection branch = new(inbound.Delivery.Forward(), inbound.Carrier);
        if (!localFirst)
        {
            await context.AcceptAsync(inbound.Delivery);
        }

        Assert.Same(inbound.Carrier, branch.Carrier);
        Assert.Same(inbound.Delivery.eventa, branch.Delivery.eventa);
        Assert.Equal(1, inbound.Delivery.hopsRemaining);
        Assert.Equal(0, branch.Delivery.hopsRemaining);
        Assert.Equal(Encoding.UTF8.GetBytes(prefix + "0" + suffix), branch.Carrier.Encode(branch.Delivery));
        Assert.Equal(Encoding.UTF8.GetBytes(prefix + "1" + suffix), inbound.Carrier.Encode(inbound.Delivery));
        // The check compares preserved tokens/spacing/escapes, not merely parsed semantic equality.
    }

    [Fact]
    public void Restore_WithVanillaTypeScriptProjection_RequiresExplicitContentType()
    {
        DeliveryInput vanilla = new(null, "remote", 1, new Eventa("event", new object()));
        Assert.Throws<ArgumentException>(() => Delivery.Restore(vanilla));
        Delivery mapped = Delivery.Restore(vanilla with { contentType = "application/example" });
        Assert.Same(vanilla.eventa, mapped.eventa);
    }

    private sealed class UninspectableBody
    {
        public static object BusinessValue => throw new InvalidOperationException("Core must not decode properties.");
        public override string ToString() => throw new InvalidOperationException("Core must not format bodies.");
        public override int GetHashCode() => throw new InvalidOperationException("Core must not hash bodies.");
        public override bool Equals(object? obj) =>
            throw new InvalidOperationException("Core must not compare bodies.");
    }

    private sealed record RuntimeValue(int Number);

    private sealed record RetainedProjection(Delivery Delivery, TestCarrier Carrier);

    private sealed record TestCarrier(byte[] Prefix, byte[] Suffix)
    {
        public byte[] Encode(Delivery forwarded)
        {
            byte[] hops = Encoding.UTF8.GetBytes(forwarded.hopsRemaining.ToString(CultureInfo.InvariantCulture));
            return [.. Prefix, .. hops, .. Suffix];
        }
    }
}
