namespace Eventa;

/// <summary>
/// Controls when the client dispatches request traffic after subscriptions and cancellation are armed.
/// </summary>
internal enum SendDispatchMode
{
    /// <summary>
    /// Sends inline once local cancellation is armed so a queued send cannot outrun an early abort.
    /// </summary>
    InlineAfterCancellationArmed,

    /// <summary>
    /// Queues request pumping to the thread pool, which is suitable for async request streams.
    /// </summary>
    QueueOnThreadPool,
}

/// <summary>
/// Owns the shared client-side invoke lifecycle: correlation by invoke id, response subscriptions,
/// cancellation arming, request dispatch, and terminal cleanup.
/// </summary>
/// <remarks>
/// Derived engines choose the terminal projection for the session, such as <see cref="Task{TResult}"/>
/// for unary invokes or <see cref="IAsyncEnumerable{T}"/> for streamed responses.
/// </remarks>
internal abstract class InvokeSessionEngine<TResponse, TRequest>(
    IEventContext context,
    InvokeEventBindings<TResponse, TRequest> events,
    SendDispatchMode sendDispatchMode,
    Func<string, CancellationToken, Task> sendRequest,
    CancellationToken cancellationToken)
{
    private readonly Func<string, CancellationToken, Task> _sendRequest = sendRequest;
    private readonly SendDispatchMode _sendDispatchMode = sendDispatchMode;
    private readonly CancellationToken _cancellationToken = cancellationToken;
    private readonly CancellationTokenSource _requestCancellationSource = cancellationToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : new CancellationTokenSource();
    private readonly List<IDisposable> _subscriptions = [];
    private readonly string _invokeId = IdGenerator.New();
    private int _finished;

    protected IEventContext Context { get; } = context;

    protected InvokeEventBindings<TResponse, TRequest> Events { get; } = events;

    protected string InvokeId => _invokeId;

    protected CancellationToken CancellationToken => _cancellationToken;

    protected bool IsFinished => Volatile.Read(ref _finished) != 0;

    /// <summary>
    /// Subscribes the invoke-specific success channel and forwards matching responses.
    /// </summary>
    protected void SubscribeToReceive(Action<TResponse> onReceive)
    {
        _subscriptions.Add(Context.Subscribe(Events.Receive, envelope =>
        {
            if (!StringComparer.Ordinal.Equals(envelope.Body.InvokeId, _invokeId)) return;

            onReceive(envelope.Body.Content);
        }));
    }

    /// <summary>
    /// Subscribes the invoke-specific error channel and forwards matching terminal errors.
    /// </summary>
    protected void SubscribeToReceiveError(Action<Exception> onError)
    {
        _subscriptions.Add(Context.Subscribe(Events.ReceiveError, envelope =>
        {
            if (!StringComparer.Ordinal.Equals(envelope.Body.InvokeId, _invokeId)) return;

            onError(envelope.Body.Error);
        }));
    }

    /// <summary>
    /// Subscribes the invoke-specific response stream end channel.
    /// </summary>
    protected void SubscribeToReceiveStreamEnd(Action onStreamEnd)
    {
        _subscriptions.Add(Context.Subscribe(Events.ReceiveStreamEnd, envelope =>
        {
            if (!StringComparer.Ordinal.Equals(envelope.Body.InvokeId, _invokeId)) return;

            onStreamEnd();
        }));
    }

    /// <summary>
    /// Subscribes any configured fatal events that should immediately fault this session.
    /// </summary>
    protected void SubscribeToFatalEvents(Action<Exception> onError)
    {
        if (!TryGetInvokeInternalConfig(Context, out var internalConfig)) return;

        foreach (var fatalEvent in internalConfig.AbortOnEvents)
        {
            if (IsFinished) return;

            var subscription = new DeferredDisposable();
            _subscriptions.Add(subscription);

            subscription.Attach(fatalEvent.Subscribe(Context, error =>
            {
                onError(error ?? CreateAbortException());
            }));
        }
    }

    /// <summary>
    /// Arms local cancellation after protocol subscriptions are in place, then dispatches the request.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when the session is still active after request dispatch begins; otherwise,
    /// <see langword="false"/> when cancellation or another terminal event already won the race.
    /// </returns>
    protected bool TryStart(Action onCancellation, Action<Exception> onSendFault)
    {
        if (IsFinished) return false;

        if (!ClientCancellation.TryArm(_subscriptions, onCancellation, _cancellationToken))
        {
            return false;
        }

        DispatchSendRequest(onCancellation, onSendFault);
        return !IsFinished;
    }

    /// <summary>
    /// Transitions the session to its terminal state once, optionally emitting the protocol abort event.
    /// </summary>
    protected void Finish(bool emitAbort, Action complete)
    {
        if (Interlocked.Exchange(ref _finished, 1) != 0) return;

        _requestCancellationSource.Cancel();

        if (emitAbort)
        {
            Context.Emit(Events.SendAbort, new AbortPayload(_invokeId));
        }

        complete();
        Cleanup();
        _requestCancellationSource.Dispose();
    }

    /// <summary>
    /// Dispatches the request inline or on the thread pool after cancellation is fully armed.
    /// </summary>
    private void DispatchSendRequest(Action onCancellation, Action<Exception> onSendFault)
    {
        if (IsFinished) return;

        if (_cancellationToken.IsCancellationRequested)
        {
            onCancellation();
            return;
        }

        if (_sendDispatchMode is SendDispatchMode.InlineAfterCancellationArmed)
        {
            ExecuteSendRequestAsync(onSendFault).GetAwaiter().GetResult();
            return;
        }

        _ = Task.Run(() => ExecuteSendRequestAsync(onSendFault), CancellationToken.None);
    }

    /// <summary>
    /// Executes the request sender and converts local send failures into terminal faults.
    /// </summary>
    private async Task ExecuteSendRequestAsync(Action<Exception> onSendFault)
    {
        try
        {
            await _sendRequest(_invokeId, _requestCancellationSource.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_requestCancellationSource.Token.IsCancellationRequested) { return; }
        catch (Exception error)
        {
            onSendFault(error);
        }
    }

    /// <summary>
    /// Disposes every subscription and deferred registration owned by this session.
    /// </summary>
    private void Cleanup()
    {
        foreach (var subscription in _subscriptions)
        {
            subscription.Dispose();
        }
    }

    /// <summary>
    /// Resolves the invoke extension state for fatal-event subscriptions when it is available.
    /// </summary>
    private static bool TryGetInvokeInternalConfig(
        IEventContext context,
        out InvokeInternalConfig config)
    {
        if (context.Extensions.TryGetValue(InvokeExtensions.InternalInvokeConfigKey, out var rawConfig)
            && rawConfig is InvokeInternalConfig internalConfig)
        {
            config = internalConfig;
            return true;
        }

        config = null!;
        return false;
    }

    /// <summary>
    /// Creates the fallback exception used when a fatal event does not supply an error payload.
    /// </summary>
    private static InvalidOperationException CreateAbortException()
    {
        return new InvalidOperationException("Pending invoke aborted by fatal event.");
    }
}

/// <summary>
/// Projects the shared invoke session lifecycle onto a single terminal response task.
/// </summary>
internal sealed class UnaryInvokeSessionEngine<TResponse, TRequest>(
    IEventContext context,
    InvokeEventBindings<TResponse, TRequest> events,
    SendDispatchMode sendDispatchMode,
    Func<string, CancellationToken, Task> sendRequest,
    CancellationToken cancellationToken) : InvokeSessionEngine<TResponse, TRequest>(context, events, sendDispatchMode, sendRequest, cancellationToken)
{
    private readonly TaskCompletionSource<TResponse> _completion =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// Starts the invoke session and completes when a response, error, cancellation, or fatal event arrives.
    /// </summary>
    public Task<TResponse> Run()
    {
        SubscribeToReceive(CompleteSuccessfully);
        SubscribeToReceiveError(CompleteFaulted);
        SubscribeToFatalEvents(CompleteFaulted);
        TryStart(AbortFromClient, CompleteFaulted);
        return _completion.Task;
    }

    /// <summary>
    /// Completes the invoke task with the received response payload.
    /// </summary>
    private void CompleteSuccessfully(TResponse response)
    {
        Finish(emitAbort: false, () => _completion.TrySetResult(response));
    }

    /// <summary>
    /// Faults the invoke task with a protocol, fatal-event, or local send error.
    /// </summary>
    private void CompleteFaulted(Exception error)
    {
        Finish(emitAbort: false, () => _completion.TrySetException(error));
    }

    /// <summary>
    /// Cancels the invoke from the client side and emits the protocol abort event.
    /// </summary>
    private void AbortFromClient()
    {
        Finish(emitAbort: true, () => _completion.TrySetCanceled(CancellationToken));
    }
}

/// <summary>
/// Projects the shared invoke session lifecycle onto a buffered async response stream.
/// </summary>
/// <remarks>
/// Stream sessions intentionally remain protocol-driven and do not subscribe to fatal abort events so
/// their behavior stays aligned with the pre-refactor client contract.
/// </remarks>
internal sealed class StreamInvokeSessionEngine<TResponse, TRequest>(
    IEventContext context,
    InvokeEventBindings<TResponse, TRequest> events,
    SendDispatchMode sendDispatchMode,
    Func<string, CancellationToken, Task> sendRequest,
    CancellationToken cancellationToken) : InvokeSessionEngine<TResponse, TRequest>(context, events, sendDispatchMode, sendRequest, cancellationToken)
{
    private readonly AsyncSignalQueue<TResponse> _responses = new();

    /// <summary>
    /// Starts the invoke session and exposes the response side as an async sequence.
    /// </summary>
    public IAsyncEnumerable<TResponse> Run()
    {
        SubscribeToReceive(response => _responses.TryWrite(response));
        SubscribeToReceiveError(Fault);
        SubscribeToReceiveStreamEnd(Complete);

        var isActive = TryStart(AbortFromClient, Fault);
        return CreateResultStream(isActive ? AbortOnDisposeAsync : NoopOnDisposeAsync);
    }

    /// <summary>
    /// Exposes the buffered response queue as an async sequence with session-specific disposal behavior.
    /// </summary>
    private IAsyncEnumerable<TResponse> CreateResultStream(Func<ValueTask> onDispose)
    {
        return _responses.ReadAll(onDispose: onDispose);
    }

    /// <summary>
    /// Completes the response stream normally.
    /// </summary>
    private void Complete()
    {
        FinishStream(error: null, emitAbort: false);
    }

    /// <summary>
    /// Faults the response stream with a protocol or local send error.
    /// </summary>
    private void Fault(Exception error)
    {
        FinishStream(error, emitAbort: false);
    }

    /// <summary>
    /// Aborts the response stream from the client token and surfaces cancellation locally.
    /// </summary>
    private void AbortFromClient()
    {
        FinishStream(new OperationCanceledException(CancellationToken), emitAbort: true);
    }

    /// <summary>
    /// Completes or faults the buffered response stream and optionally emits the protocol abort event.
    /// </summary>
    private void FinishStream(Exception? error, bool emitAbort)
    {
        Finish(emitAbort, () =>
        {
            if (error is null)
            {
                _responses.Complete();
            }
            else
            {
                _responses.Fault(error);
            }
        });
    }

    /// <summary>
    /// Aborts the invoke when the consumer stops enumeration before the session reaches a terminal state.
    /// </summary>
    private ValueTask AbortOnDisposeAsync()
    {
        FinishStream(error: null, emitAbort: true);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Leaves the session untouched when cleanup already happened before enumeration was exposed.
    /// </summary>
    private static ValueTask NoopOnDisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
