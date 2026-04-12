namespace Eventa;

public sealed record SendPayload<TRequest>(string InvokeId, TRequest Content);

public sealed record SendErrorPayload(string InvokeId, Exception Error);

public sealed record StreamEndPayload(string InvokeId);

public sealed record AbortPayload(string InvokeId, string? Reason = null);

public sealed record ReceivePayload<TResponse>(string InvokeId, TResponse Content);

public sealed record ReceiveErrorPayload(string InvokeId, Exception Error);
