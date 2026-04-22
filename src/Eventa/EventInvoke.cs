using System.Runtime.CompilerServices;

namespace Eventa;

public static class EventInvoke
{
    private static readonly ConditionalWeakTable<IEventContext, InvokeHandlerRegistry> HandlerRegistries = [];

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
        {
            var context = contextFactory();
            var invokeId = IdGenerator.New();
            var sendEvent = new EventDefinition<SendPayload<TRequest>>(eventDefinition.SendEventId);
            var sendAbortEvent = new EventDefinition<AbortPayload>(eventDefinition.SendAbortId);
            var receiveEvent = new EventDefinition<ReceivePayload<TResponse>>(eventDefinition.ReceiveEventId);
            var receiveErrorEvent = new EventDefinition<ReceiveErrorPayload>(eventDefinition.ReceiveErrorId);
            var completion = new TaskCompletionSource<TResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
            var disposables = new List<IDisposable>();
            var finished = 0;

            void Cleanup()
            {
                foreach (var disposable in disposables)
                {
                    disposable.Dispose();
                }
            }

            void CompleteSuccessfully(TResponse response)
            {
                if (Interlocked.Exchange(ref finished, 1) != 0) return;

                completion.TrySetResult(response);
                Cleanup();
            }

            void CompleteFaulted(Exception error)
            {
                if (Interlocked.Exchange(ref finished, 1) != 0) return;

                completion.TrySetException(error);
                Cleanup();
            }

            void FinishCanceled(bool emitAbort)
            {
                if (Interlocked.Exchange(ref finished, 1) != 0) return;

                if (emitAbort)
                {
                    context.Emit(sendAbortEvent, new AbortPayload(invokeId));
                }

                completion.TrySetCanceled(cancellationToken);
                Cleanup();
            }

            void AbortFromClient()
            {
                FinishCanceled(emitAbort: true);
            }

            bool TryArmClientCancellation()
            {
                if (!cancellationToken.CanBeCanceled) return true;

                if (cancellationToken.IsCancellationRequested)
                {
                    AbortFromClient();
                    return false;
                }

                var cancellationRegistration = new DeferredCancellationRegistration();
                disposables.Add(cancellationRegistration);
                cancellationRegistration.Attach(cancellationToken.Register(AbortFromClient));

                // Cancellation can still win the race between the pre-check and Register.
                if (cancellationToken.IsCancellationRequested)
                {
                    AbortFromClient();
                    return false;
                }

                return true;
            }

            disposables.Add(context.On(receiveEvent, envelope =>
            {
                if (!StringComparer.Ordinal.Equals(envelope.Body.InvokeId, invokeId)) return;

                CompleteSuccessfully(envelope.Body.Content);
            }));

            disposables.Add(context.On(receiveErrorEvent, envelope =>
            {
                if (!StringComparer.Ordinal.Equals(envelope.Body.InvokeId, invokeId)) return;

                CompleteFaulted(envelope.Body.Error);
            }));

            if (TryGetInvokeInternalConfig(context, out var internalConfig))
            {
                foreach (var fatalEvent in internalConfig.AbortOnEvents)
                {
                    disposables.Add(fatalEvent.Subscribe(context, error =>
                    {
                        CompleteFaulted(error ?? CreateAbortException());
                    }));
                }
            }

            if (!TryArmClientCancellation())
            {
                return completion.Task;
            }

            context.Emit(sendEvent, new SendPayload<TRequest>(invokeId, request));
            return completion.Task;
        };
    }

    public static IDisposable DefineInvokeHandler<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<TRequest, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        var registry = HandlerRegistries.GetValue(context, static _ => new InvokeHandlerRegistry());

        lock (registry.SyncRoot)
        {
            if (!registry.Registrations.TryGetValue(eventDefinition.SendEventId, out var handlers))
            {
                handlers = [];
                registry.Registrations[eventDefinition.SendEventId] = handlers;
            }

            if (!handlers.TryGetValue(handler, out HandlerRegistration? registration))
            {
                registration = CreateUnaryHandlerRegistration(context, eventDefinition, handler);
                handlers[handler] = registration;
            }
        }

        return new ActionDisposable(() => RemoveHandlerRegistration(registry, eventDefinition.SendEventId, handler));
    }

    public static IDisposable DefineInvokeHandler<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<IAsyncEnumerable<TRequest>, CancellationToken, Task<TResponse>> handler)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(eventDefinition);
        ArgumentNullException.ThrowIfNull(handler);

        var registry = HandlerRegistries.GetValue(context, static _ => new InvokeHandlerRegistry());

        lock (registry.SyncRoot)
        {
            if (!registry.Registrations.TryGetValue(eventDefinition.SendEventId, out var handlers))
            {
                handlers = [];
                registry.Registrations[eventDefinition.SendEventId] = handlers;
            }

            if (!handlers.TryGetValue(handler, out HandlerRegistration? registration))
            {
                registration = CreateRequestStreamHandlerRegistration(context, eventDefinition, handler);
                handlers[handler] = registration;
            }
        }

        return new ActionDisposable(() => RemoveHandlerRegistration(registry, eventDefinition.SendEventId, handler));
    }

    private static HandlerRegistration CreateUnaryHandlerRegistration<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<TRequest, CancellationToken, Task<TResponse>> handler)
    {
        var sendEvent = new EventDefinition<SendPayload<TRequest>>(eventDefinition.SendEventId);
        var sendAbortEvent = new EventDefinition<AbortPayload>(eventDefinition.SendAbortId);
        var receiveEvent = new EventDefinition<ReceivePayload<TResponse>>(eventDefinition.ReceiveEventId);
        var receiveErrorEvent = new EventDefinition<ReceiveErrorPayload>(eventDefinition.ReceiveErrorId);
        var sync = new object();
        var inflight = new Dictionary<string, CancellationTokenSource>(StringComparer.Ordinal);

        async Task HandleInvokeAsync(string invokeId, TRequest request)
        {
            var cancellationSource = new CancellationTokenSource();

            lock (sync)
            {
                inflight[invokeId] = cancellationSource;
            }

            try
            {
                var response = await handler(request, cancellationSource.Token).ConfigureAwait(false);
                if (!cancellationSource.IsCancellationRequested)
                {
                    context.Emit(receiveEvent, new ReceivePayload<TResponse>(invokeId, response));
                }
            }
            catch (OperationCanceledException) when (cancellationSource.IsCancellationRequested) { return; }
            catch (Exception error)
            {
                if (!cancellationSource.IsCancellationRequested)
                {
                    context.Emit(receiveErrorEvent, new ReceiveErrorPayload(invokeId, error));
                }
            }
            finally
            {
                lock (sync)
                {
                    inflight.Remove(invokeId);
                }

                cancellationSource.Dispose();
            }
        }

        var subscriptions = new List<IDisposable>
        {
            context.On(sendEvent, envelope => _ = HandleInvokeAsync(envelope.Body.InvokeId, envelope.Body.Content)),
            context.On(sendAbortEvent, envelope =>
            {
                lock (sync)
                {
                    if (inflight.TryGetValue(envelope.Body.InvokeId, out var cancellationSource))
                    {
                        cancellationSource.Cancel();
                    }
                }
            }),
        };

        return new HandlerRegistration(subscriptions, () =>
        {
            lock (sync)
            {
                foreach (var cancellationSource in inflight.Values)
                {
                    cancellationSource.Cancel();
                    cancellationSource.Dispose();
                }

                inflight.Clear();
            }
        });
    }

    private static HandlerRegistration CreateRequestStreamHandlerRegistration<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<IAsyncEnumerable<TRequest>, CancellationToken, Task<TResponse>> handler)
    {
        var sendEvent = new EventDefinition<SendPayload<TRequest>>(eventDefinition.SendEventId);
        var sendStreamEndEvent = new EventDefinition<StreamEndPayload>(eventDefinition.SendStreamEndId);
        var sendAbortEvent = new EventDefinition<AbortPayload>(eventDefinition.SendAbortId);
        var receiveEvent = new EventDefinition<ReceivePayload<TResponse>>(eventDefinition.ReceiveEventId);
        var receiveErrorEvent = new EventDefinition<ReceiveErrorPayload>(eventDefinition.ReceiveErrorId);
        var sync = new object();
        var inflight = new Dictionary<string, RequestStreamInvocationState<TRequest>>(StringComparer.Ordinal);

        RequestStreamInvocationState<TRequest> GetOrCreateState(string invokeId)
        {
            lock (sync)
            {
                if (inflight.TryGetValue(invokeId, out var existing)) return existing;

                var created = new RequestStreamInvocationState<TRequest>(invokeId);
                inflight[invokeId] = created;
                created.Execution = HandleInvokeAsync(created);
                return created;
            }
        }

        async Task HandleInvokeAsync(RequestStreamInvocationState<TRequest> state)
        {
            try
            {
                var response = await handler(
                    state.Requests.ReadAll(respectConsumerCancellation: false),
                    state.CancellationSource.Token).ConfigureAwait(false);

                if (state.CancellationSource.IsCancellationRequested) return;

                context.Emit(receiveEvent, new ReceivePayload<TResponse>(state.InvokeId, response));
            }
            catch (OperationCanceledException) when (state.CancellationSource.IsCancellationRequested) { return; }
            catch (Exception error)
            {
                if (state.CancellationSource.IsCancellationRequested) return;

                context.Emit(receiveErrorEvent, new ReceiveErrorPayload(state.InvokeId, error));
            }
            finally
            {
                lock (sync)
                {
                    inflight.Remove(state.InvokeId);
                }

                state.CancellationSource.Dispose();
            }
        }

        var subscriptions = new List<IDisposable>
        {
            context.On(sendEvent, envelope =>
            {
                var state = GetOrCreateState(envelope.Body.InvokeId);
                state.Requests.TryWrite(envelope.Body.Content);
            }),
            context.On(sendStreamEndEvent, envelope =>
            {
                // Keep empty request streams as a supported C# contract; current
                // TypeScript invoke.ts also materializes unknown invokeIds here.
                var state = GetOrCreateState(envelope.Body.InvokeId);
                state.Requests.Complete();
            }),
            context.On(sendAbortEvent, envelope =>
            {
                // Keep pre-first-item aborts as a supported C# contract; current
                // TypeScript invoke.ts also materializes unknown invokeIds on abort
                // so the handler still observes cancellation.
                var state = GetOrCreateState(envelope.Body.InvokeId);
                state.Requests.Fault(new OperationCanceledException(state.CancellationSource.Token));
                state.CancellationSource.Cancel();
            }),
        };

        return new HandlerRegistration(subscriptions, () =>
        {
            lock (sync)
            {
                foreach (var state in inflight.Values)
                {
                    state.Requests.Fault(new OperationCanceledException(state.CancellationSource.Token));
                    state.CancellationSource.Cancel();
                    state.CancellationSource.Dispose();
                }

                inflight.Clear();
            }
        });
    }

    private static void RemoveHandlerRegistration(
        InvokeHandlerRegistry registry,
        string eventId,
        Delegate handler)
    {
        HandlerRegistration? registration = null;

        lock (registry.SyncRoot)
        {
            if (!registry.Registrations.TryGetValue(eventId, out var handlers)
                || !handlers.TryGetValue(handler, out registration)) return;

            handlers.Remove(handler);
            if (handlers.Count == 0)
            {
                registry.Registrations.Remove(eventId);
            }
        }

        registration.Dispose();
    }

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
        public object SyncRoot { get; } = new();

        public Dictionary<string, Dictionary<Delegate, HandlerRegistration>> Registrations { get; } =
            new(StringComparer.Ordinal);
    }

    private sealed class HandlerRegistration(
        IReadOnlyCollection<IDisposable> subscriptions,
        Action cleanup)
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

            foreach (var subscription in subscriptions)
            {
                subscription.Dispose();
            }

            cleanup();
        }
    }

    private sealed class RequestStreamInvocationState<TRequest>(string invokeId)
    {
        public string InvokeId { get; } = invokeId;

        public AsyncSignalQueue<TRequest> Requests { get; } = new();

        public CancellationTokenSource CancellationSource { get; } = new();

        public Task? Execution { get; set; }
    }
}
