namespace Eventa.Tests;

public class EventContextTests
{
    [Fact]
    public void Subscribe_And_Emit_DispatchEventEnvelope()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        EventEnvelope<TestPayload>? received = null;

        using var _ = context.Subscribe(definition, envelope => received = envelope);

        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(
            new EventEnvelope<TestPayload>("test-event", new TestPayload("test")),
            received);
    }

    [Fact]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0039:Use local function", Justification = "Keep the handler in a variable so the same handler value is subscribed twice.")]
    public void Subscribe_DeduplicatesTheSameHandlerInstance()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var callCount = 0;
        Action<EventEnvelope<TestPayload>> handler = _ => callCount++;

        using var _ = context.Subscribe(definition, handler);
        using var __ = context.Subscribe(definition, handler);

        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(1, callCount);
    }

    [Fact]
    public void SubscribeOnce_OnlyDispatchesTheFirstMatchingEvent()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var callCount = 0;
        EventEnvelope<TestPayload>? firstEnvelope = null;

        using var _ = context.SubscribeOnce(definition, envelope =>
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
    public void SubscribeOnce_WhenHandlerEmitsReentrantly_StillDispatchesOnlyOnce()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var callCount = 0;

        using var _ = context.SubscribeOnce(definition, _ =>
        {
            callCount++;
            context.Emit(definition, new TestPayload("reentrant"));
        });

        context.Emit(definition, new TestPayload("first"));

        Assert.Equal(1, callCount);
    }

    [Fact]
    public void Unsubscribe_WithoutHandler_RemovesAllListenersForTheEvent()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var callCount = 0;

        using var _ = context.Subscribe(definition, _ => callCount++);

        context.Unsubscribe(definition);
        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(0, callCount);
    }

    [Fact]
    public void ReturnedSubscription_DisposesTheListener()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var callCount = 0;

        var subscription = context.Subscribe(definition, _ => callCount++);

        subscription.Dispose();
        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(0, callCount);
    }

    [Fact]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0039:Use local function", Justification = "Keep handlers in variables so Off can remove one specific handler value.")]
    public void Unsubscribe_WithHandler_RemovesOnlyTheRequestedListener()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var strongCalls = 0;
        var weakCalls = 0;
        Action<EventEnvelope<TestPayload>> strongHandler = _ => strongCalls++;
        Action<EventEnvelope<TestPayload>> weakHandler = _ => weakCalls++;

        using var _ = context.Subscribe(definition, strongHandler);
        using var __ = context.Subscribe(definition, weakHandler);

        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(1, strongCalls);
        Assert.Equal(1, weakCalls);

        context.Unsubscribe(definition, weakHandler);
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

        using var _ = context.Subscribe(definition, strongHandler);
        var weakSubscription = context.Subscribe(definition, weakHandler);

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

        using var _ = context.Subscribe(expression, envelope => matchedValues.Add(envelope.Body.Value));

        context.Emit(definition, new TestPayload("match-first"));
        context.Emit(definition, new TestPayload("skip"));
        context.Emit(definition, new TestPayload("match-second"));

        Assert.Equal(["match-first", "match-second"], matchedValues);
    }

    [Fact]
    public void SubscribeOnce_WithMatchExpression_OnlyDispatchesTheFirstMatchingEvent()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var expression = new MatchExpression<TestPayload>(
            "starts-with-match-once",
            envelope => envelope.Body.Value.StartsWith("match", StringComparison.Ordinal));
        var matchedValues = new List<string>();

        using var _ = context.SubscribeOnce(expression, envelope => matchedValues.Add(envelope.Body.Value));

        context.Emit(definition, new TestPayload("skip"));
        context.Emit(definition, new TestPayload("match-first"));
        context.Emit(definition, new TestPayload("match-second"));

        Assert.Equal(["match-first"], matchedValues);
    }

    [Fact]
    public void SubscribeOnce_WithMatchExpression_WhenHandlerEmitsReentrantly_StillDispatchesOnlyOnce()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var expression = new MatchExpression<TestPayload>("match-all-once", _ => true);
        var callCount = 0;

        using var _ = context.SubscribeOnce(expression, _ =>
        {
            callCount++;
            context.Emit(definition, new TestPayload("reentrant"));
        });

        context.Emit(definition, new TestPayload("first"));

        Assert.Equal(1, callCount);
    }

    [Fact]
    public void Unsubscribe_WithMatchExpressionAndNoHandler_RemovesAllListenersForTheExpression()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var expression = new MatchExpression<TestPayload>("match-all", _ => true);
        var callCount = 0;

        using var _ = context.Subscribe(expression, _ => callCount++);
        using var __ = context.SubscribeOnce(expression, _ => callCount++);

        context.Unsubscribe(expression);
        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(0, callCount);
    }

    [Fact]
    public void Unsubscribe_WithMatchExpressionAndHandler_RemovesOnlyTheRequestedListener()
    {
        var context = new EventContext();
        var definition = new EventDefinition<TestPayload>("test-event");
        var expression = new MatchExpression<TestPayload>("match-all", _ => true);
        var strongCalls = 0;
        var weakCalls = 0;
        Action<EventEnvelope<TestPayload>> strongHandler = _ => strongCalls++;
        Action<EventEnvelope<TestPayload>> weakHandler = _ => weakCalls++;

        using var _ = context.Subscribe(expression, strongHandler);
        using var __ = context.Subscribe(expression, weakHandler);

        context.Unsubscribe(expression, weakHandler);
        context.Emit(definition, new TestPayload("test"));

        Assert.Equal(1, strongCalls);
        Assert.Equal(0, weakCalls);
    }

    [Fact]
    public void Emit_CallsAdapterOnSent_AfterLocalListeners()
    {
        var calls = new List<string>();
        using var adapter = new RecordingAdapter(calls);
        using var context = new EventContext(adapter);
        var definition = new EventDefinition<TestPayload>("test-event");
        var expression = new MatchExpression<TestPayload>("match-test-event", _ => true);

        using var _ = context.Subscribe(definition, _ => calls.Add("listener"));
        using var __ = context.Subscribe(expression, _ => calls.Add("match"));

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

        using var _ = context.Subscribe(definition, _ => { });

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

        using var _ = context.Subscribe(expression, _ => { });

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

        using var _ = context.Subscribe(definition, _ => throw new InvalidOperationException("boom"));

        var error = Assert.Throws<InvalidOperationException>(() => context.Emit(definition, new TestPayload("test")));

        Assert.Equal("boom", error.Message);
        Assert.Empty(calls);
    }

    [Fact]
    public void Subscribe_WhenEventIdAlreadyBoundToDifferentPayloadType_ThrowsClearException()
    {
        var context = new EventContext();
        var firstDefinition = new EventDefinition<FirstPayload>("shared-event");
        var secondDefinition = new EventDefinition<SecondPayload>("shared-event");

        using var _ = context.Subscribe(firstDefinition, _ => { });

        var error = Assert.Throws<InvalidOperationException>(() => context.Subscribe(secondDefinition, _ => { }));

        AssertPayloadTypeInvariant(
            error,
            bindingTarget: nameof(EventDefinition<>),
            id: "shared-event",
            boundType: typeof(FirstPayload),
            currentType: typeof(SecondPayload),
            operation: "Subscribe");
    }

    [Fact]
    public void SubscribeOnce_WhenEventIdAlreadyBoundToDifferentPayloadType_ThrowsClearException()
    {
        var context = new EventContext();
        var firstDefinition = new EventDefinition<FirstPayload>("shared-event");
        var secondDefinition = new EventDefinition<SecondPayload>("shared-event");

        using var _ = context.SubscribeOnce(firstDefinition, _ => { });

        var error = Assert.Throws<InvalidOperationException>(() => context.SubscribeOnce(secondDefinition, _ => { }));

        AssertPayloadTypeInvariant(
            error,
            bindingTarget: nameof(EventDefinition<>),
            id: "shared-event",
            boundType: typeof(FirstPayload),
            currentType: typeof(SecondPayload),
            operation: "SubscribeOnce");
    }

    [Fact]
    public void Emit_WhenEventIdAlreadyBoundToDifferentPayloadType_ThrowsClearException()
    {
        var context = new EventContext();
        var firstDefinition = new EventDefinition<FirstPayload>("shared-event");
        var secondDefinition = new EventDefinition<SecondPayload>("shared-event");

        using var _ = context.Subscribe(firstDefinition, _ => { });

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

        var error = Assert.Throws<InvalidOperationException>(() => context.Subscribe(secondDefinition, _ => { }));

        AssertPayloadTypeInvariant(
            error,
            bindingTarget: nameof(EventDefinition<>),
            id: "shared-event",
            boundType: typeof(FirstPayload),
            currentType: typeof(SecondPayload),
            operation: "Subscribe");
    }

    [Fact]
    public void Emit_WithOptions_FirstUseBindsEventIdPayloadType()
    {
        var context = new EventContext();
        var firstDefinition = new EventDefinition<FirstPayload>("shared-event");
        var secondDefinition = new EventDefinition<SecondPayload>("shared-event");

        context.Emit(firstDefinition, new FirstPayload("first"), new EmitOptions("test"));

        var error = Assert.Throws<InvalidOperationException>(() => context.Subscribe(secondDefinition, _ => { }));

        AssertPayloadTypeInvariant(
            error,
            bindingTarget: nameof(EventDefinition<>),
            id: "shared-event",
            boundType: typeof(FirstPayload),
            currentType: typeof(SecondPayload),
            operation: "Subscribe");
    }

    [Fact]
    public void Unsubscribe_WhenEventIdAlreadyBoundToDifferentPayloadType_ThrowsClearException()
    {
        var context = new EventContext();
        var firstDefinition = new EventDefinition<FirstPayload>("shared-event");
        var secondDefinition = new EventDefinition<SecondPayload>("shared-event");

        using var _ = context.Subscribe(firstDefinition, _ => { });

        var error = Assert.Throws<InvalidOperationException>(() => context.Unsubscribe(secondDefinition));

        AssertPayloadTypeInvariant(
            error,
            bindingTarget: nameof(EventDefinition<>),
            id: "shared-event",
            boundType: typeof(FirstPayload),
            currentType: typeof(SecondPayload),
            operation: "Unsubscribe");
    }

    [Fact]
    public void Unsubscribe_DoesNotReleaseEventIdPayloadTypeBinding()
    {
        var context = new EventContext();
        var firstDefinition = new EventDefinition<FirstPayload>("shared-event");
        var secondDefinition = new EventDefinition<SecondPayload>("shared-event");

        using var _ = context.Subscribe(firstDefinition, _ => { });

        context.Unsubscribe(firstDefinition);

        var error = Assert.Throws<InvalidOperationException>(() => context.Subscribe(secondDefinition, _ => { }));

        AssertPayloadTypeInvariant(
            error,
            bindingTarget: nameof(EventDefinition<>),
            id: "shared-event",
            boundType: typeof(FirstPayload),
            currentType: typeof(SecondPayload),
            operation: "Subscribe");
    }

    [Fact]
    public void EventIdPayloadTypeBinding_IsScopedToEachContext()
    {
        var firstDefinition = new EventDefinition<FirstPayload>("shared-event");
        var secondDefinition = new EventDefinition<SecondPayload>("shared-event");
        var secondContextCalls = 0;

        using (var firstContext = new EventContext())
        {
            using var _ = firstContext.Subscribe(firstDefinition, _ => { });
        }

        using var secondContext = new EventContext();
        using var __ = secondContext.Subscribe(secondDefinition, _ => secondContextCalls++);

        secondContext.Emit(secondDefinition, new SecondPayload(2));

        Assert.Equal(1, secondContextCalls);
    }

    [Fact]
    public void Subscribe_WhenMatchExpressionIdAlreadyBoundToDifferentPayloadType_ThrowsClearException()
    {
        var context = new EventContext();
        var firstExpression = new MatchExpression<FirstPayload>("shared-match", _ => true);
        var secondExpression = new MatchExpression<SecondPayload>("shared-match", _ => true);

        using var _ = context.Subscribe(firstExpression, _ => { });

        var error = Assert.Throws<InvalidOperationException>(() => context.Subscribe(secondExpression, _ => { }));

        AssertPayloadTypeInvariant(
            error,
            bindingTarget: nameof(MatchExpression<>),
            id: "shared-match",
            boundType: typeof(FirstPayload),
            currentType: typeof(SecondPayload),
            operation: "Subscribe");
    }

    [Fact]
    public void SubscribeOnce_WhenMatchExpressionIdAlreadyBoundToDifferentPayloadType_ThrowsClearException()
    {
        var context = new EventContext();
        var firstExpression = new MatchExpression<FirstPayload>("shared-match-once", _ => true);
        var secondExpression = new MatchExpression<SecondPayload>("shared-match-once", _ => true);

        using var _ = context.SubscribeOnce(firstExpression, _ => { });

        var error = Assert.Throws<InvalidOperationException>(() => context.SubscribeOnce(secondExpression, _ => { }));

        AssertPayloadTypeInvariant(
            error,
            bindingTarget: nameof(MatchExpression<>),
            id: "shared-match-once",
            boundType: typeof(FirstPayload),
            currentType: typeof(SecondPayload),
            operation: "SubscribeOnce");
    }

    [Fact]
    public void Unsubscribe_WithMatchExpressionDoesNotReleasePayloadTypeBinding()
    {
        var context = new EventContext();
        var firstExpression = new MatchExpression<FirstPayload>("shared-match-unsubscribe", _ => true);
        var secondExpression = new MatchExpression<SecondPayload>("shared-match-unsubscribe", _ => true);

        using var _ = context.Subscribe(firstExpression, _ => { });

        context.Unsubscribe(firstExpression);

        var error = Assert.Throws<InvalidOperationException>(() => context.Subscribe(secondExpression, _ => { }));

        AssertPayloadTypeInvariant(
            error,
            bindingTarget: nameof(MatchExpression<>),
            id: "shared-match-unsubscribe",
            boundType: typeof(FirstPayload),
            currentType: typeof(SecondPayload),
            operation: "Subscribe");
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
