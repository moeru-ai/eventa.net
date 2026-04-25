namespace Eventa;

/// <summary>
/// Represents a reusable client for invoke contracts that complete with a single response.
/// </summary>
/// <typeparam name="TResponse">The response payload type returned by each invoke.</typeparam>
/// <typeparam name="TRequest">The request payload type sent by each invoke.</typeparam>
/// <remarks>
/// Each call allocates a fresh invoke id, subscribes to the derived response channels for that id,
/// and completes when a response, protocol error, cancellation, or fatal adapter event arrives.
/// </remarks>
public sealed class InvokeClient<TResponse, TRequest>
{
    private readonly Func<IEventContext> _contextFactory;
    private readonly InvokeEventBindings<TResponse, TRequest> _events;

    internal InvokeClient(
        Func<IEventContext> contextFactory,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition)
    {
        _contextFactory = contextFactory;
        _events = new InvokeEventBindings<TResponse, TRequest>(eventDefinition);
    }

    /// <summary>
    /// Sends one request and awaits one terminal response for the bound invoke contract.
    /// </summary>
    /// <param name="request">The request payload to send to the handler.</param>
    /// <param name="cancellationToken">
    /// A token that aborts the invoke locally and emits the protocol abort event when canceled.
    /// </param>
    /// <returns>A task that resolves to the handler response payload.</returns>
    public Task<TResponse> InvokeAsync(
        TRequest request,
        CancellationToken cancellationToken = default)
    {
        return new PendingInvokeOperation(
            _contextFactory(),
            _events,
            request,
            cancellationToken).Run();
    }

    /// <summary>
    /// Encapsulates one pending unary invoke from subscription setup through terminal cleanup.
    /// </summary>
    /// <param name="context">The context that carries the invoke protocol traffic.</param>
    /// <param name="events">The concrete protocol events for the invoke contract.</param>
    /// <param name="request">The request payload that should be sent to the handler.</param>
    /// <param name="cancellationToken">The client token that can abort the invoke.</param>
    private sealed class PendingInvokeOperation(
        IEventContext context,
        InvokeEventBindings<TResponse, TRequest> events,
        TRequest request,
        CancellationToken cancellationToken)
    {
        private readonly string _invokeId = IdGenerator.New();
        private readonly TaskCompletionSource<TResponse> _completion =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly List<IDisposable> _subscriptions = [];
        private int _finished;

        /// <summary>
        /// Arms protocol subscriptions, wires cancellation, and dispatches the request when still active.
        /// </summary>
        /// <returns>The task that completes when the invoke resolves, faults, or is canceled.</returns>
        public Task<TResponse> Run()
        {
            SubscribeToResponses();
            SubscribeToFatalEvents();

            if (Volatile.Read(ref _finished) != 0)
            {
                return _completion.Task;
            }

            if (!ClientCancellation.TryArm(_subscriptions, AbortFromClient, cancellationToken))
            {
                return _completion.Task;
            }

            EmitRequest();
            return _completion.Task;
        }

        /// <summary>
        /// Subscribes the invoke-specific success and error response channels.
        /// </summary>
        private void SubscribeToResponses()
        {
            _subscriptions.Add(context.Subscribe(events.Receive, envelope =>
            {
                if (!StringComparer.Ordinal.Equals(envelope.Body.InvokeId, _invokeId)) return;

                CompleteSuccessfully(envelope.Body.Content);
            }));

            _subscriptions.Add(context.Subscribe(events.ReceiveError, envelope =>
            {
                if (!StringComparer.Ordinal.Equals(envelope.Body.InvokeId, _invokeId)) return;

                CompleteFaulted(envelope.Body.Error);
            }));
        }

        /// <summary>
        /// Subscribes any configured fatal events that should immediately fault this invoke.
        /// </summary>
        private void SubscribeToFatalEvents()
        {
            if (!TryGetInvokeInternalConfig(context, out var internalConfig)) return;

            foreach (var fatalEvent in internalConfig.AbortOnEvents)
            {
                if (Volatile.Read(ref _finished) != 0) return;

                var subscription = new DeferredDisposable();
                _subscriptions.Add(subscription);

                subscription.Attach(fatalEvent.Subscribe(context, error =>
                {
                    CompleteFaulted(error ?? CreateAbortException());
                }));
            }
        }

        /// <summary>
        /// Emits the invoke request after subscriptions and client cancellation are fully armed.
        /// </summary>
        private void EmitRequest()
        {
            // Fatal events can finish the invoke after subscriptions are armed but before send.
            if (Volatile.Read(ref _finished) != 0) return;

            // Cancellation can still win after TryArm returns; route it through the abort path.
            if (cancellationToken.IsCancellationRequested)
            {
                AbortFromClient();
                return;
            }

            context.Emit(events.Send, new SendPayload<TRequest>(_invokeId, request));
        }

        /// <summary>
        /// Disposes every subscription or deferred registration owned by this pending invoke.
        /// </summary>
        private void Cleanup()
        {
            foreach (var subscription in _subscriptions)
            {
                subscription.Dispose();
            }
        }

        /// <summary>
        /// Completes the invoke successfully without emitting a protocol abort.
        /// </summary>
        /// <param name="response">The response payload received from the handler.</param>
        private void CompleteSuccessfully(TResponse response)
        {
            Finish(emitAbort: false, () => _completion.TrySetResult(response));
        }

        /// <summary>
        /// Completes the invoke with the supplied protocol or fatal error.
        /// </summary>
        /// <param name="error">The exception that should fault the invoke task.</param>
        private void CompleteFaulted(Exception error)
        {
            Finish(emitAbort: false, () => _completion.TrySetException(error));
        }

        /// <summary>
        /// Completes the invoke as canceled and optionally notifies the remote handler via abort.
        /// </summary>
        /// <param name="emitAbort">
        /// <see langword="true"/> to emit the protocol abort event before completing locally.
        /// </param>
        private void FinishCanceled(bool emitAbort)
        {
            Finish(emitAbort, () => _completion.TrySetCanceled(cancellationToken));
        }

        /// <summary>
        /// Transitions the invoke to its terminal state once, optionally emitting a protocol abort.
        /// </summary>
        /// <param name="emitAbort">
        /// <see langword="true"/> to notify the remote side that the client aborted the invoke.
        /// </param>
        /// <param name="complete">The completion action that sets the final task state.</param>
        private void Finish(bool emitAbort, Action complete)
        {
            if (Interlocked.Exchange(ref _finished, 1) != 0) return;

            if (emitAbort)
            {
                context.Emit(events.SendAbort, new AbortPayload(_invokeId));
            }

            complete();
            Cleanup();
        }

        /// <summary>
        /// Aborts the invoke from the client side and publishes the protocol abort event.
        /// </summary>
        private void AbortFromClient()
        {
            FinishCanceled(emitAbort: true);
        }

        /// <summary>
        /// Resolves the per-context invoke extension state when fatal-event aborts are configured.
        /// </summary>
        /// <param name="context">The context whose extension bag should be inspected.</param>
        /// <param name="config">The resolved invoke configuration when available.</param>
        /// <returns><see langword="true"/> when invoke extension state exists on the context.</returns>
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
        /// Creates the fallback exception used when a fatal event aborts the invoke without an error payload.
        /// </summary>
        private static InvalidOperationException CreateAbortException()
        {
            return new InvalidOperationException("Pending invoke aborted by fatal event.");
        }
    }
}
