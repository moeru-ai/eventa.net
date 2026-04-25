namespace Eventa;

internal static class InvokeStreamHandlerRegistrationFactory
{
    public static HandlerRegistration CreateUnary<TResponse, TRequest>(
        IEventContext context,
        InvokeEventDefinition<TResponse, TRequest> eventDefinition,
        Func<TRequest, CancellationToken, IAsyncEnumerable<TResponse>> handler)
    {
        var events = new InvokeEventBindings<TResponse, TRequest>(eventDefinition);
        var inflight = new InvocationCancellationTracker();

        async Task<bool> TryForwardResponsesAsync(
            string invokeId,
            CancellationTokenSource cancellationSource,
            IAsyncEnumerable<TResponse> responses)
        {
            // Avoid starting handler-stream enumeration after an early abort won.
            if (cancellationSource.IsCancellationRequested) return false;

            await foreach (var item in responses.ConfigureAwait(false))
            {
                // Cancellation can win after MoveNextAsync but before we emit this item.
                if (cancellationSource.IsCancellationRequested) return false;

                context.Emit(events.Receive, new ReceivePayload<TResponse>(invokeId, item));
            }

            // Do not send stream-end if cancellation arrived after the last item.
            if (cancellationSource.IsCancellationRequested) return false;

            context.Emit(events.ReceiveStreamEnd, new StreamEndPayload(invokeId));
            return true;
        }

        async Task HandleInvokeAsync(string invokeId, TRequest request)
        {
            var cancellationSource = inflight.BeginTracking(invokeId);

            try
            {
                // Skip handler activation entirely when abort won before startup.
                if (cancellationSource.IsCancellationRequested) return;

                var responses = handler(request, cancellationSource.Token);
                var forwarded = await TryForwardResponsesAsync(invokeId, cancellationSource, responses);
                if (!forwarded) return;
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
        Func<IAsyncEnumerable<TRequest>, CancellationToken, IAsyncEnumerable<TResponse>> handler)
    {
        var events = new InvokeEventBindings<TResponse, TRequest>(eventDefinition);
        var inflight = new RequestStreamInvocationTracker<TRequest>();

        RequestStreamInvocationState<TRequest> GetOrCreateState(string invokeId)
        {
            return inflight.GetOrCreate(invokeId, HandleInvokeAsync);
        }

        async Task<bool> TryForwardResponsesAsync(
            RequestStreamInvocationState<TRequest> state,
            IAsyncEnumerable<TResponse> responses)
        {
            // Avoid starting handler-stream enumeration after an early abort won.
            if (state.CancellationSource.IsCancellationRequested) return false;

            await foreach (var item in responses.ConfigureAwait(false))
            {
                // Cancellation can win after MoveNextAsync but before we emit this item.
                if (state.CancellationSource.IsCancellationRequested) return false;

                context.Emit(events.Receive, new ReceivePayload<TResponse>(state.InvokeId, item));
            }

            // Do not send stream-end if cancellation arrived after the last item.
            if (state.CancellationSource.IsCancellationRequested) return false;

            context.Emit(events.ReceiveStreamEnd, new StreamEndPayload(state.InvokeId));
            return true;
        }

        async Task HandleInvokeAsync(RequestStreamInvocationState<TRequest> state)
        {
            try
            {
                // Skip handler activation entirely when abort won before startup.
                if (state.CancellationSource.IsCancellationRequested) return;

                var responses = handler(
                    state.Requests.ReadAll(respectConsumerCancellation: false),
                    state.CancellationSource.Token);
                var forwarded = await TryForwardResponsesAsync(state, responses);
                if (!forwarded) return;
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
                // Keep empty request streams as a supported C# contract even though
                // current TypeScript stream.ts ignores unknown invokeIds here.
                GetOrCreateState(envelope.Body.InvokeId).Requests.Complete();
            }),
            context.Subscribe(events.SendAbort, envelope =>
            {
                // Keep pre-first-item aborts as a supported C# contract; current
                // TypeScript stream.ts also materializes unknown invokeIds on abort
                // so the handler still observes cancellation.
                GetOrCreateState(envelope.Body.InvokeId).Abort();
            }),
        };

        return new HandlerRegistration(subscriptions, inflight.AbortAllAndDispose);
    }
}
