namespace Eventa.Tests;

public class EventContextTests
{
    [Fact]
    public void On_and_Emit_DispatchEventEnvelope()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        EventEnvelope<TestPayload>? received = null;

        using var _ = context.On(definition, envelope => received = envelope);

        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(
            new EventEnvelope<TestPayload>("test-event", new TestPayload("test")),
            received);
    }

    [Fact]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0039:Use local function", Justification = "Keep the handler in a variable so the same handler value is subscribed twice.")]
    public void On_DeduplicatesTheSameHandlerInstance()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var callCount = 0;
        Action<EventEnvelope<TestPayload>> handler = _ => callCount++;

        using var _ = context.On(definition, handler);
        using var __ = context.On(definition, handler);

        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(1, callCount);
    }

    [Fact]
    public void Once_OnlyDispatchesTheFirstMatchingEvent()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var callCount = 0;
        EventEnvelope<TestPayload>? firstEnvelope = null;

        using var _ = context.Once(definition, envelope =>
        {
            callCount++;
            firstEnvelope = envelope;
        });

        context.Emit(definition, new TestPayload("first"));
        context.Emit(definition, new TestPayload("second"));

        Assert.Equal(1, callCount);
        Assert.Equal(
            new EventEnvelope<TestPayload>("test-event", new TestPayload("first")),
            firstEnvelope);
    }

    [Fact]
    public void Off_WithoutHandler_RemovesAllListenersForTheEvent()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var callCount = 0;

        using var _ = context.On(definition, _ => callCount++);

        context.Off(definition);
        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(0, callCount);
    }

    [Fact]
    public void ReturnedSubscription_DisposesTheListener()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var callCount = 0;

        var subscription = context.On(definition, _ => callCount++);

        subscription.Dispose();
        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(0, callCount);
    }

    [Fact]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0039:Use local function", Justification = "Keep handlers in variables so Off can remove one specific handler value.")]
    public void Off_WithHandler_RemovesOnlyTheRequestedListener()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var strongCalls = 0;
        var weakCalls = 0;
        Action<EventEnvelope<TestPayload>> strongHandler = _ => strongCalls++;
        Action<EventEnvelope<TestPayload>> weakHandler = _ => weakCalls++;

        using var _ = context.On(definition, strongHandler);
        using var __ = context.On(definition, weakHandler);

        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(1, strongCalls);
        Assert.Equal(1, weakCalls);

        context.Off(definition, weakHandler);
        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(2, strongCalls);
        Assert.Equal(1, weakCalls);
    }

    [Fact]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0039:Use local function", Justification = "Keep handlers in variables so one subscription stays tied to one handler value.")]
    public void ReturnedSubscription_RemovesOnlyTheRequestedListener()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var strongCalls = 0;
        var weakCalls = 0;
        Action<EventEnvelope<TestPayload>> strongHandler = _ => strongCalls++;
        Action<EventEnvelope<TestPayload>> weakHandler = _ => weakCalls++;

        using var _ = context.On(definition, strongHandler);
        var weakSubscription = context.On(definition, weakHandler);

        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(1, strongCalls);
        Assert.Equal(1, weakCalls);

        weakSubscription.Dispose();
        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(2, strongCalls);
        Assert.Equal(1, weakCalls);
    }

    [Fact]
    public void MatchExpressionSubscriptions_ReceiveOnlyMatchingPayloads()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var expression = new MatchExpression<TestPayload>(
            "starts-with-match",
            envelope => envelope.Body.Value.StartsWith("match", StringComparison.Ordinal));
        var matchedValues = new List<string>();

        using var _ = context.On(expression, envelope => matchedValues.Add(envelope.Body.Value));

        context.Emit(definition, new TestPayload("match-first"));
        context.Emit(definition, new TestPayload("skip"));
        context.Emit(definition, new TestPayload("match-second"));

        Assert.Equal(["match-first", "match-second"], matchedValues);
    }

    [Fact]
    public void Emit_CallsAdapterOnSent_AfterLocalListeners()
    {
        var calls = new List<string>();
        using var adapter = new RecordingAdapter(calls);
        using var context = new EventContext(adapter);
        var definition = new EventDefinition<TestPayload>("test-event");
        var expression = new MatchExpression<TestPayload>("match-test-event", _ => true);

        using var _ = context.On(definition, _ => calls.Add("listener"));
        using var __ = context.On(expression, _ => calls.Add("match"));

        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(
            ["listener", "received:test-event", "match", "received:match-test-event", "sent:test-event"],
            calls);
    }

    [Fact]
    public void Emit_CallsAdapterWithEventEnvelopePayloads()
    {
        var calls = new List<string>();
        using var adapter = new RecordingAdapter(calls);
        using var context = new EventContext(adapter);
        var definition = new EventDefinition<TestPayload>("test-event");

        using var _ = context.On(definition, _ => { });

        context.Emit(definition, new TestPayload("test"));

        var received = Assert.Single(adapter.ReceivedCalls);
        Assert.Equal("test-event", received.EventId);
        Assert.Equal(
            new EventEnvelope<TestPayload>("test-event", new TestPayload("test")),
            Assert.IsType<EventEnvelope<TestPayload>>(received.Envelope));

        var sent = Assert.Single(adapter.SentCalls);
        Assert.Equal("test-event", sent.EventId);
        Assert.Equal(
            new EventEnvelope<TestPayload>("test-event", new TestPayload("test")),
            Assert.IsType<EventEnvelope<TestPayload>>(sent.Envelope));
    }

    [Fact]
    public void Emit_WithMatchExpression_CallsAdapterOnReceivedWithMatchId_AndEnvelopeKeepsOriginalEventId()
    {
        var calls = new List<string>();
        using var adapter = new RecordingAdapter(calls);
        using var context = new EventContext(adapter);
        var definition = new EventDefinition<TestPayload>("test-event");
        var expression = new MatchExpression<TestPayload>("match-test-event", _ => true);

        using var _ = context.On(expression, _ => { });

        context.Emit(definition, new TestPayload("test"));

        var received = Assert.Single(adapter.ReceivedCalls);
        Assert.Equal("match-test-event", received.EventId);

        var envelope = Assert.IsType<EventEnvelope<TestPayload>>(received.Envelope);
        Assert.Equal("test-event", envelope.EventId);
        Assert.Equal(new TestPayload("test"), envelope.Body);
    }

    [Fact]
    public void Emit_WhenListenerThrows_DoesNotCallAdapterOnSent()
    {
        var calls = new List<string>();
        using var adapter = new RecordingAdapter(calls);
        using var context = new EventContext(adapter);
        var definition = new EventDefinition<TestPayload>("test-event");

        using var _ = context.On(definition, _ => throw new InvalidOperationException("boom"));

        var error = Assert.Throws<InvalidOperationException>(() => context.Emit(definition, new TestPayload("test")));

        Assert.Equal("boom", error.Message);
        Assert.Empty(calls);
    }

    [Fact]
    public void On_WhenEventIdAlreadyBoundToDifferentPayloadType_ThrowsClearException()
    {
        var context = new EventContext();
        var firstDefinition = new EventDefinition<FirstPayload>("shared-event");
        var secondDefinition = new EventDefinition<SecondPayload>("shared-event");

        using var _ = context.On(firstDefinition, _ => { });

        var error = Assert.Throws<InvalidOperationException>(() => context.On(secondDefinition, _ => { }));

        AssertPayloadTypeInvariant(
            error,
            bindingTarget: nameof(EventDefinition<>),
            id: "shared-event",
            boundType: typeof(FirstPayload),
            currentType: typeof(SecondPayload),
            operation: "On");
    }

    [Fact]
    public void Once_WhenEventIdAlreadyBoundToDifferentPayloadType_ThrowsClearException()
    {
        var context = new EventContext();
        var firstDefinition = new EventDefinition<FirstPayload>("shared-event");
        var secondDefinition = new EventDefinition<SecondPayload>("shared-event");

        using var _ = context.Once(firstDefinition, _ => { });

        var error = Assert.Throws<InvalidOperationException>(() => context.Once(secondDefinition, _ => { }));

        AssertPayloadTypeInvariant(
            error,
            bindingTarget: nameof(EventDefinition<>),
            id: "shared-event",
            boundType: typeof(FirstPayload),
            currentType: typeof(SecondPayload),
            operation: "Once");
    }

    [Fact]
    public void Emit_WhenEventIdAlreadyBoundToDifferentPayloadType_ThrowsClearException()
    {
        var context = new EventContext();
        var firstDefinition = new EventDefinition<FirstPayload>("shared-event");
        var secondDefinition = new EventDefinition<SecondPayload>("shared-event");

        using var _ = context.On(firstDefinition, _ => { });

        var error = Assert.Throws<InvalidOperationException>(() => context.Emit(secondDefinition, new SecondPayload(2)));

        AssertPayloadTypeInvariant(
            error,
            bindingTarget: nameof(EventDefinition<>),
            id: "shared-event",
            boundType: typeof(FirstPayload),
            currentType: typeof(SecondPayload),
            operation: "Emit");
    }

    [Fact]
    public void Emit_FirstUseBindsEventIdPayloadType()
    {
        var context = new EventContext();
        var firstDefinition = new EventDefinition<FirstPayload>("shared-event");
        var secondDefinition = new EventDefinition<SecondPayload>("shared-event");

        context.Emit(firstDefinition, new FirstPayload("first"));

        var error = Assert.Throws<InvalidOperationException>(() => context.On(secondDefinition, _ => { }));

        AssertPayloadTypeInvariant(
            error,
            bindingTarget: nameof(EventDefinition<>),
            id: "shared-event",
            boundType: typeof(FirstPayload),
            currentType: typeof(SecondPayload),
            operation: "On");
    }

    [Fact]
    public void Emit_WithOptions_FirstUseBindsEventIdPayloadType()
    {
        var context = new EventContext();
        var firstDefinition = new EventDefinition<FirstPayload>("shared-event");
        var secondDefinition = new EventDefinition<SecondPayload>("shared-event");

        context.Emit(firstDefinition, new FirstPayload("first"), new EmitOptions("test"));

        var error = Assert.Throws<InvalidOperationException>(() => context.On(secondDefinition, _ => { }));

        AssertPayloadTypeInvariant(
            error,
            bindingTarget: nameof(EventDefinition<>),
            id: "shared-event",
            boundType: typeof(FirstPayload),
            currentType: typeof(SecondPayload),
            operation: "On");
    }

    [Fact]
    public void Off_WhenEventIdAlreadyBoundToDifferentPayloadType_ThrowsClearException()
    {
        var context = new EventContext();
        var firstDefinition = new EventDefinition<FirstPayload>("shared-event");
        var secondDefinition = new EventDefinition<SecondPayload>("shared-event");

        using var _ = context.On(firstDefinition, _ => { });

        var error = Assert.Throws<InvalidOperationException>(() => context.Off(secondDefinition));

        AssertPayloadTypeInvariant(
            error,
            bindingTarget: nameof(EventDefinition<>),
            id: "shared-event",
            boundType: typeof(FirstPayload),
            currentType: typeof(SecondPayload),
            operation: "Off");
    }

    [Fact]
    public void Off_DoesNotReleaseEventIdPayloadTypeBinding()
    {
        var context = new EventContext();
        var firstDefinition = new EventDefinition<FirstPayload>("shared-event");
        var secondDefinition = new EventDefinition<SecondPayload>("shared-event");

        using var _ = context.On(firstDefinition, _ => { });

        context.Off(firstDefinition);

        var error = Assert.Throws<InvalidOperationException>(() => context.On(secondDefinition, _ => { }));

        AssertPayloadTypeInvariant(
            error,
            bindingTarget: nameof(EventDefinition<>),
            id: "shared-event",
            boundType: typeof(FirstPayload),
            currentType: typeof(SecondPayload),
            operation: "On");
    }

    [Fact]
    public void EventIdPayloadTypeBinding_IsScopedToEachContext()
    {
        var firstDefinition = new EventDefinition<FirstPayload>("shared-event");
        var secondDefinition = new EventDefinition<SecondPayload>("shared-event");
        var secondContextCalls = 0;

        using (var firstContext = new EventContext())
        {
            using var _ = firstContext.On(firstDefinition, _ => { });
        }

        using var secondContext = new EventContext();
        using var __ = secondContext.On(secondDefinition, _ => secondContextCalls++);

        secondContext.Emit(secondDefinition, new SecondPayload(2));

        Assert.Equal(1, secondContextCalls);
    }

    [Fact]
    public void On_WhenMatchExpressionIdAlreadyBoundToDifferentPayloadType_ThrowsClearException()
    {
        var context = new EventContext();
        var firstExpression = new MatchExpression<FirstPayload>("shared-match", _ => true);
        var secondExpression = new MatchExpression<SecondPayload>("shared-match", _ => true);

        using var _ = context.On(firstExpression, _ => { });

        var error = Assert.Throws<InvalidOperationException>(() => context.On(secondExpression, _ => { }));

        AssertPayloadTypeInvariant(
            error,
            bindingTarget: nameof(MatchExpression<>),
            id: "shared-match",
            boundType: typeof(FirstPayload),
            currentType: typeof(SecondPayload),
            operation: "On");
    }

    private static void AssertPayloadTypeInvariant(
        InvalidOperationException error,
        string bindingTarget,
        string id,
        Type boundType,
        Type currentType,
        string operation)
    {
        Assert.Contains(bindingTarget, error.Message, StringComparison.Ordinal);
        Assert.Contains(id, error.Message, StringComparison.Ordinal);
        Assert.Contains(boundType.ToString(), error.Message, StringComparison.Ordinal);
        Assert.Contains(currentType.ToString(), error.Message, StringComparison.Ordinal);
        Assert.Contains(operation, error.Message, StringComparison.Ordinal);
    }

    private sealed record TestPayload(string Value);
    private sealed record FirstPayload(string Value);
    private sealed record SecondPayload(int Value);
    private sealed record EmitOptions(string Source);
    private sealed record AdapterSentCall(string EventId, object? Envelope, object? Options);
    private sealed record AdapterReceivedCall(string EventId, object? Envelope);

    private sealed class RecordingAdapter(List<string> calls) : IEventaAdapter
    {
        public List<AdapterSentCall> SentCalls { get; } = [];

        public List<AdapterReceivedCall> ReceivedCalls { get; } = [];

        public void OnSent(string eventId, object? envelope, object? options = null)
        {
            SentCalls.Add(new AdapterSentCall(eventId, envelope, options));
            calls.Add($"sent:{eventId}");
        }

        public void OnReceived(string eventId, object? envelope)
        {
            ReceivedCalls.Add(new AdapterReceivedCall(eventId, envelope));
            calls.Add($"received:{eventId}");
        }

#pragma warning disable CA1822 // Mark members as static
        public void Dispose() { }
#pragma warning restore CA1822 // Mark members as static
    }
}
