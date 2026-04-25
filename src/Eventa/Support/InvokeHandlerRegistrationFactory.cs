namespace Eventa;

internal static class InvokeHandlerRegistrationFactory
{
    public static HandlerRegistration CreateUnary<TResponse, TRequest>(
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
            context.Subscribe(events.Send, envelope => _ = HandleInvokeAsync(envelope.Body.InvokeId, envelope.Body.Content)),
            context.Subscribe(events.SendAbort, envelope => inflight.TryCancel(envelope.Body.InvokeId)),
        };

        return new HandlerRegistration(subscriptions, inflight.CancelAllAndDispose);
    }

    public static HandlerRegistration CreateRequestStream<TResponse, TRequest>(
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
            context.Subscribe(events.Send, envelope =>
            {
                GetOrCreateState(envelope.Body.InvokeId).Requests.TryWrite(envelope.Body.Content);
            }),
            context.Subscribe(events.SendStreamEnd, envelope =>
            {
                // Keep empty request streams as a supported C# contract; current
                // TypeScript invoke.ts also materializes unknown invokeIds here.
                GetOrCreateState(envelope.Body.InvokeId).Requests.Complete();
            }),
            context.Subscribe(events.SendAbort, envelope =>
            {
                // Keep pre-first-item aborts as a supported C# contract; current
                // TypeScript invoke.ts also materializes unknown invokeIds on abort
                // so the handler still observes cancellation.
                GetOrCreateState(envelope.Body.InvokeId).Abort();
            }),
        };

        return new HandlerRegistration(subscriptions, inflight.AbortAllAndDispose);
    }
}
