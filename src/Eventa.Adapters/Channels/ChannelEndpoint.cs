using System.Threading.Channels;

using Eventa;

namespace Eventa.Adapters.Channels;

/// <summary>
/// Represents one side of an in-process channel transport and exposes Eventa context operations.
/// </summary>
public sealed class ChannelEndpoint : IEventContext
{
    private readonly ChannelReader<ChannelMessage> _inbound;
    private readonly ChannelWriter<ChannelMessage> _outbound;
    private readonly ChannelEndpointOptions _options;
    private readonly CancellationTokenSource _disposeCancellation = new();
    private readonly ChannelAdapter _adapter;
    private readonly EventContext _context;
    private Exception? _terminalError;
    private int _terminal;

    /// <summary>
    /// Creates a channel endpoint over supplied inbound and outbound channel primitives.
    /// </summary>
    /// <param name="inbound">The reader used for remote inbound messages.</param>
    /// <param name="outbound">The writer used for local outbound messages.</param>
    /// <param name="options">Optional endpoint configuration.</param>
    public ChannelEndpoint(
        ChannelReader<ChannelMessage> inbound,
        ChannelWriter<ChannelMessage> outbound,
        ChannelEndpointOptions? options = null)
    {
        _inbound = inbound ?? throw new ArgumentNullException(nameof(inbound));
        _outbound = outbound ?? throw new ArgumentNullException(nameof(outbound));
        _options = options ?? new ChannelEndpointOptions();
        _adapter = new ChannelAdapter(_outbound);
        _context = new EventContext(_adapter);
        _ = Task.Run(RunInboundPumpAsync);
    }

    /// <inheritdoc />
    public IDictionary<string, object> Extensions => _context.Extensions;

    /// <inheritdoc />
    public void Emit<TPayload>(EventDefinition<TPayload> eventDefinition, TPayload payload)
    {
        ThrowIfTerminal();
        _context.Emit(eventDefinition, payload);
    }

    /// <inheritdoc />
    public void Emit<TPayload, TOptions>(
        EventDefinition<TPayload> eventDefinition,
        TPayload payload,
        TOptions options)
        where TOptions : class
    {
        ThrowIfTerminal();
        _context.Emit(eventDefinition, payload, options);
    }

    /// <inheritdoc />
    public IDisposable Subscribe<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>> handler)
    {
        ThrowIfTerminal();
        return _context.Subscribe(eventDefinition, handler);
    }

    /// <inheritdoc />
    public IDisposable SubscribeOnce<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>> handler)
    {
        ThrowIfTerminal();
        return _context.SubscribeOnce(eventDefinition, handler);
    }

    /// <inheritdoc />
    public void Unsubscribe<TPayload>(
        EventDefinition<TPayload> eventDefinition,
        Action<EventEnvelope<TPayload>>? handler = null)
    {
        ThrowIfTerminal();
        _context.Unsubscribe(eventDefinition, handler);
    }

    /// <inheritdoc />
    public IDisposable Subscribe<TPayload>(
        MatchExpression<TPayload> matchExpression,
        Action<EventEnvelope<TPayload>> handler)
    {
        ThrowIfTerminal();
        return _context.Subscribe(matchExpression, handler);
    }

    /// <inheritdoc />
    public IDisposable SubscribeOnce<TPayload>(
        MatchExpression<TPayload> matchExpression,
        Action<EventEnvelope<TPayload>> handler)
    {
        ThrowIfTerminal();
        return _context.SubscribeOnce(matchExpression, handler);
    }

    /// <inheritdoc />
    public void Unsubscribe<TPayload>(
        MatchExpression<TPayload> matchExpression,
        Action<EventEnvelope<TPayload>>? handler = null)
    {
        ThrowIfTerminal();
        _context.Unsubscribe(matchExpression, handler);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Terminate(
            new ChannelClosedException("Channel endpoint disposed."),
            completeOutbound: _options.CompleteOutboundOnDispose,
            cancelInbound: true);
    }

    /// <summary>
    /// Reads transport messages from the inbound channel and dispatches them into the local context.
    /// </summary>
    /// <returns>A task that completes when the inbound channel closes or the endpoint terminates.</returns>
    private async Task RunInboundPumpAsync()
    {
        try
        {
            await foreach (var message in _inbound.ReadAllAsync(_disposeCancellation.Token).ConfigureAwait(false))
            {
                if (message.Envelope is null)
                {
                    throw new InvalidOperationException("Channel message envelope cannot be null.");
                }

                _context.Receive(message.Envelope, message.Options);
            }

            Terminate(
                new ChannelClosedException("Channel closed."),
                completeOutbound: false,
                cancelInbound: false);
        }
        catch (OperationCanceledException) when (Volatile.Read(ref _terminal) != 0) { }
        catch (Exception error)
        {
            Terminate(error, completeOutbound: false, cancelInbound: false);
        }
    }

    /// <summary>
    /// Transitions the endpoint to a terminal state and releases transport resources.
    /// </summary>
    /// <param name="error">The terminal error exposed to pending invocations and future operations.</param>
    /// <param name="completeOutbound">Whether to complete the outbound channel writer.</param>
    /// <param name="cancelInbound">Whether to cancel the inbound pump.</param>
    private void Terminate(
        Exception error,
        bool completeOutbound,
        bool cancelInbound)
    {
        if (Interlocked.CompareExchange(ref _terminal, 1, 0) != 0) return;

        _terminalError = error;
        _adapter.Dispose();

        if (completeOutbound)
        {
            _outbound.TryComplete();
        }

        if (cancelInbound)
        {
            _disposeCancellation.Cancel();
        }

        _context.NotifyTransportFatal(error);
        DispatchClosedEvent(error);
        _context.Dispose();
    }

    /// <summary>
    /// Dispatches the configured closed event to local subscribers after termination.
    /// </summary>
    /// <param name="error">The terminal error carried by the closed event payload.</param>
    private void DispatchClosedEvent(Exception error)
    {
        try
        {
            _context.Receive(
                new EventEnvelope<ChannelClosedPayload>(
                    _options.ClosedEvent.Id,
                    new ChannelClosedPayload(error)));
        }
        catch { }
    }

    /// <summary>
    /// Throws when this endpoint has already reached its terminal state.
    /// </summary>
    /// <exception cref="ChannelClosedException">Thrown when the endpoint is closed.</exception>
    private void ThrowIfTerminal()
    {
        if (Volatile.Read(ref _terminal) == 0) return;

        var error = _terminalError;
        if (error is ChannelClosedException closed)
        {
            throw closed;
        }

        if (error is null)
        {
            throw new ChannelClosedException("Channel endpoint closed.");
        }

        throw new ChannelClosedException("Channel endpoint closed.", error);
    }
}
