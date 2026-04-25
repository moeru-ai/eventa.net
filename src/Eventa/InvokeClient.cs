namespace Eventa;

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

        private void Cleanup()
        {
            foreach (var subscription in _subscriptions)
            {
                subscription.Dispose();
            }
        }

        private void CompleteSuccessfully(TResponse response)
        {
            Finish(emitAbort: false, () => _completion.TrySetResult(response));
        }

        private void CompleteFaulted(Exception error)
        {
            Finish(emitAbort: false, () => _completion.TrySetException(error));
        }

        private void FinishCanceled(bool emitAbort)
        {
            Finish(emitAbort, () => _completion.TrySetCanceled(cancellationToken));
        }

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

        private void AbortFromClient()
        {
            FinishCanceled(emitAbort: true);
        }

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

        private static InvalidOperationException CreateAbortException()
        {
            return new InvalidOperationException("Pending invoke aborted by fatal event.");
        }
    }
}
