using System.Runtime.CompilerServices;

namespace Eventa;

public static class EventInvoke
{
    private static readonly ConditionalWeakTable<IEventContext, InvokeHandlerRegistry> HandlerRegistries = [];

    // Client invoke API

    public static Func<TRequest, CancellationToken, Task<TResponse>> DefineInvoke<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);

        return DefineInvoke(() => context, eventDefinition);
    }

    public static Func<TRequest, CancellationToken, Task<TResponse>> DefineInvoke<TResponse, TRequest>(
        Func<IEventContext> contextFactory,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition)
    {
        ArgumentNullException.ThrowIfNull(contextFactory);
        ArgumentNullException.ThrowIfNull(eventDefinition);

        return (request, cancellationToken) =>
            new PendingInvokeOperation<TResponse, TRequest>(
                contextFactory(),
                new InvokeEventBindings<TResponse, TRequest>(eventDefinition),
                request,
                cancellationToken).Run();
    }

    // Handler registration API

    public static IDisposable DefineInvokeHandler<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        return HandlerRegistries
            .GetValue(context, static _ => new InvokeHandlerRegistry())
            .Register(
                eventDefinition.SendEventId,
                handler,
                () => CreateUnaryHandlerRegistration(context, eventDefinition, handler));
    }

    public static IDisposable DefineInvokeHandler<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<IAsyncEnumerable<TRequest>, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        return HandlerRegistries
            .GetValue(context, static _ => new InvokeHandlerRegistry())
            .Register(
                eventDefinition.SendEventId,
                handler,
                () => CreateRequestStreamHandlerRegistration(context, eventDefinition, handler));
    }

    // Handler registration

    private static HandlerRegistration CreateUnaryHandlerRegistration<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<TRequest, CancellationToken, Task<TResponse>> handler)
    {
        var events = new InvokeEventBindings<TResponse, TRequest>(eventDefinition);
        var inflight = new InvocationCancellationTracker();

        async Task HandleInvokeAsync(string invokeId, TRequest request)
        {
            var cancellationSource = inflight.BeginTracking(invokeId);

            try
            {
                // Skip handler activation entirely when abort won before startup.
                if (cancellationSource.IsCancellationRequested) return;

                var response = await handler(request, cancellationSource.Token).ConfigureAwait(false);

                // Cancellation can still win while the handler is awaiting.
                if (cancellationSource.IsCancellationRequested) return;

                context.Emit(events.Receive, new ReceivePayload<TResponse>(invokeId, response));
            }
            catch (OperationCanceledException) when (cancellationSource.IsCancellationRequested) { return; }
            catch (Exception error)
            {
                if (cancellationSource.IsCancellationRequested) return;

                context.Emit(events.ReceiveError, new ReceiveErrorPayload(invokeId, error));
            }
            finally
            {
                inflight.StopTracking(invokeId);
                cancellationSource.Dispose();
            }
        }

        var subscriptions = new List<IDisposable>
        {
            context.On(events.Send, envelope => _ = HandleInvokeAsync(envelope.Body.InvokeId, envelope.Body.Content)),
            context.On(events.SendAbort, envelope => inflight.TryCancel(envelope.Body.InvokeId)),
        };

        return new HandlerRegistration(subscriptions, inflight.CancelAllAndDispose);
    }

    private static HandlerRegistration CreateRequestStreamHandlerRegistration<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<IAsyncEnumerable<TRequest>, CancellationToken, Task<TResponse>> handler)
    {
        var events = new InvokeEventBindings<TResponse, TRequest>(eventDefinition);
        var inflight = new RequestStreamInvocationTracker<TRequest>();

        RequestStreamInvocationState<TRequest> GetOrCreateState(string invokeId)
        {
            return inflight.GetOrCreate(invokeId, HandleInvokeAsync);
        }

        async Task HandleInvokeAsync(RequestStreamInvocationState<TRequest> state)
        {
            try
            {
                // Skip handler activation entirely when abort won before startup.
                if (state.CancellationSource.IsCancellationRequested) return;

                var response = await handler(
                    state.Requests.ReadAll(respectConsumerCancellation: false),
                    state.CancellationSource.Token).ConfigureAwait(false);

                // Cancellation can still win while the handler is awaiting.
                if (state.CancellationSource.IsCancellationRequested) return;

                context.Emit(events.Receive, new ReceivePayload<TResponse>(state.InvokeId, response));
            }
            catch (OperationCanceledException) when (state.CancellationSource.IsCancellationRequested) { return; }
            catch (Exception error)
            {
                if (state.CancellationSource.IsCancellationRequested) return;

                context.Emit(events.ReceiveError, new ReceiveErrorPayload(state.InvokeId, error));
            }
            finally
            {
                inflight.Remove(state);
                state.Dispose();
            }
        }

        var subscriptions = new List<IDisposable>
        {
            context.On(events.Send, envelope =>
            {
                GetOrCreateState(envelope.Body.InvokeId).Requests.TryWrite(envelope.Body.Content);
            }),
            context.On(events.SendStreamEnd, envelope =>
            {
                // Keep empty request streams as a supported C# contract; current
                // TypeScript invoke.ts also materializes unknown invokeIds here.
                GetOrCreateState(envelope.Body.InvokeId).Requests.Complete();
            }),
            context.On(events.SendAbort, envelope =>
            {
                // Keep pre-first-item aborts as a supported C# contract; current
                // TypeScript invoke.ts also materializes unknown invokeIds on abort
                // so the handler still observes cancellation.
                GetOrCreateState(envelope.Body.InvokeId).Abort();
            }),
        };

        return new HandlerRegistration(subscriptions, inflight.AbortAllAndDispose);
    }

    // Client operation

    private sealed class PendingInvokeOperation<TResponse, TRequest>(
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

            if (!ClientCancellation.TryArm(cancellationToken, _subscriptions, AbortFromClient))
            {
                return _completion.Task;
            }

            EmitRequest();
            return _completion.Task;
        }

        private void SubscribeToResponses()
        {
            _subscriptions.Add(context.On(events.Receive, envelope =>
            {
                if (!StringComparer.Ordinal.Equals(envelope.Body.InvokeId, _invokeId)) return;

                CompleteSuccessfully(envelope.Body.Content);
            }));

            _subscriptions.Add(context.On(events.ReceiveError, envelope =>
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
                _subscriptions.Add(fatalEvent.Subscribe(context, error =>
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
    }

    // Support

    private static bool TryGetInvokeInternalConfig(IEventContext context, out InvokeInternalConfig config)
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

    private sealed class InvokeHandlerRegistry
    {
        private readonly Lock _sync = new();
        private readonly Dictionary<string, Dictionary<Delegate, HandlerRegistration>> _registrations =
            new(StringComparer.Ordinal);

        public IDisposable Register(
            string eventId,
            Delegate handler,
            Func<HandlerRegistration> createRegistration)
        {
            lock (_sync)
            {
                if (!_registrations.TryGetValue(eventId, out var handlers))
                {
                    handlers = [];
                    _registrations[eventId] = handlers;
                }

                if (!handlers.TryGetValue(handler, out HandlerRegistration? registration))
                {
                    registration = createRegistration();
                    handlers[handler] = registration;
                }
            }

            return new ActionDisposable(() => Remove(eventId, handler));
        }

        private void Remove(string eventId, Delegate handler)
        {
            HandlerRegistration? registration = null;

            lock (_sync)
            {
                if (!_registrations.TryGetValue(eventId, out var handlers)
                    || !handlers.TryGetValue(handler, out registration)) return;

                handlers.Remove(handler);
                if (handlers.Count == 0)
                {
                    _registrations.Remove(eventId);
                }
            }

            registration.Dispose();
        }
    }
}
