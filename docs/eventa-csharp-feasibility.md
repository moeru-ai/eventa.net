# Eventa → C# .NET 10 迁移可行性报告

## 1. Eventa 架构概览

### 1.1 Eventa 是什么

Eventa 是一个**传输无关的类型安全事件系统**，在事件原语之上组合出 RPC（请求-响应）和流式通信模式。核心理念：

- **事件是一等公民** — 定义一次，到处使用
- **传输层可插拔** — 同一事件定义可跨 Electron IPC、WebSocket、Web Worker、BroadcastChannel 等传输层工作
- **RPC 即事件** — invoke/stream 模式完全由事件原语组合而成，不引入额外协议

### 1.2 核心三层

```
┌─────────────────────────────────────────────────────────┐
│                    应用层 (Application)                    │
│  defineInvoke / defineStreamInvoke / withRemoteMethods   │
├─────────────────────────────────────────────────────────┤
│                    协议层 (Protocol)                       │
│  EventContext (emit / on / once / off)                    │
│  InvokeEventa (7 事件对)                                  │
│  MatchExpression (glob / regex / 自定义谓词)               │
├─────────────────────────────────────────────────────────┤
│                    适配器层 (Adapter)                      │
│  EventTarget / EventEmitter / WebSocket / Electron /     │
│  BroadcastChannel / WebWorker / WorkerThreads            │
└─────────────────────────────────────────────────────────┘
```

### 1.3 EventContext — 发布/订阅核心

`createContext()` 返回一个 `EventContext`，其内部维护：

| 数据结构 | 用途 |
|---------|------|
| `Map<EventId, Set<Handler>>` listeners | 持久监听器 |
| `Map<EventId, Set<Handler>>` onceListeners | 一次性监听器（触发后自动移除） |
| `Map<MatchExpressionId, MatchExpression>` matchExpressions | 匹配表达式注册表 |
| `Map<MatchExpressionId, Set<Handler>>` matchExpressionListeners | 按匹配表达式分组的监听器 |

`emit(event, payload)` 流程：

1. 构造 `emittingPayload = { ...event, body: payload }`
2. 遍历对应 `event.id` 的所有 listeners 并逐一调用
3. 遍历对应 `event.id` 的所有 onceListeners，调用后从 Set 中删除
4. 遍历所有已注册的 matchExpressions，对 emittingPayload 执行 `matcher()`，匹配成功则分发给该表达式的 listeners/onceListeners
5. 调用适配器 hook `onSent(event.id, emittingPayload, options)`

`on()` / `once()` 返回一个 `() => void` 取消订阅函数。

### 1.4 Invoke 协议 — 7 事件对

`defineInvokeEventa<Res, Req>()` 生成 7 个关联事件，前缀共享同一 `tag`：

| 事件 | 方向 | 含义 |
|------|------|------|
| `{tag}-send` | Client → Server | 发起请求（含 `invokeId` + `content`） |
| `{tag}-send-error` | Client → Server | 客户端流式输入错误 |
| `{tag}-send-stream-end` | Client → Server | 客户端流式输入结束 |
| `{tag}-send-abort` | Client → Server | 客户端取消请求 |
| `{tag}-receive` | Server → Client | 服务端响应成功 |
| `{tag}-receive-error` | Server → Client | 服务端 handler 抛出异常 |
| `{tag}-receive-stream-end` | Server → Client | 服务端流式响应结束 |

**InvokeId 关联机制**：每次调用生成 `invokeId`（nanoid 16字符），监听事件 ID 拼接为 `{eventId}-{invokeId}`，确保并发 invoke 互不干扰。

### 1.5 Invoke Handler 流程

`defineInvokeHandler(ctx, events, handler)` 的服务端内部：

1. 监听 `sendEvent`：收到请求后根据 `isReqStream` 判断是否为流式输入
   - 非流式：直接 `handleInvoke(invokeId, payload)`
   - 流式：创建 `ReadableStream`，将每个 chunk 通过 `controller.enqueue()` 推入
2. 监听 `sendEventStreamEnd`：关闭流式输入的 ReadableStream controller
3. 监听 `sendEventAbort`：abort `AbortController`，如有流则 error 掉 ReadableStream
4. `handleInvoke` 执行 handler，结果通过 `receiveEvent` 发回，异常通过 `receiveEventError` 发回
5. 返回 `() => void` 用于取消注册该 handler

### 1.6 Stream 协议

`defineStreamInvoke` / `defineStreamInvokeHandler` 与 Invoke 共享相同的 7 事件对，差异：

- **Client 侧**：返回 `ReadableStream<Res>` 而非 `Promise<Res>`，监听多个 `receiveEvent` 并 `enqueue()`，直到 `receiveEventStreamEnd` 触发 `close()`
- **Server 侧**：handler 返回 `AsyncGenerator<Res>`，每 yield 一个值就 emit 一个 `receiveEvent`，结束时 emit `receiveEventStreamEnd`
- `toStreamHandler()` 辅助函数：将回调风格（`emit(data)` 推送）转换为 `AsyncGenerator` 风格

### 1.7 适配器模式

适配器是一个函数 `(emit) => { cleanup, hooks: { onSent, onReceived } }`：

- `onSent`：`ctx.emit()` 每次调用后触发，适配器在此将事件序列化并发送到传输层
- `onReceived`：`ctx.on()`/`ctx.once()` 处理消息时触发（作为日志 hook）
- 返回 `cleanup` 函数用于断开传输层连接

已有适配器覆盖的传输层：EventTarget、EventEmitter、BroadcastChannel、WebSocket（客户端+H3服务端）、Electron（main+renderer）、WebWorker、Worker Threads。

### 1.8 扩展机制

- **Invoke 扩展**：`withRemoteMethods()` 包装 `defineInvoke`/`defineInvokeHandler`，实现 function stub 序列化/反序列化（将函数替换为 `{ __eventaInvoke: { tag } }` 标记，在另一端自动注册为新的 invoke handler）
- **Context 扩展**：`EventContext.extensions` 字段，适配器可携带内部状态（如 `__internal.invoke.abortOnEvents` 用于 worker 崩溃时自动 reject 所有 pending invoke）
- **Emit 扩展**：`EmitOptions` 泛型参数，适配器可注入额外选项（如 `{ raw: { event } }` 用于传递原始传输层事件、`{ transfer: Transferable[] }` 用于 Structured Clone 传输）

---

## 2. Eventa 测试体系分析

### 2.1 测试框架

- 使用 **Vitest** 作为测试运行器
- `vi.fn()` 创建 mock/spy 函数
- `expectTypeOf()` 进行编译期类型断言
- 纯单元测试，所有测试在同一进程内使用 `createContext()` 直连（无真实传输层）

### 2.2 各测试文件覆盖场景

#### `context.spec.ts` — EventContext 基础

| 场景 | 描述 |
|------|------|
| register and emit | 注册 handler 后 emit，验证 handler 收到 `{ ...event, body: payload }` |
| same handler only once | 同一 handler 注册两次，emit 时只调用一次（Set 去重） |
| once listeners | `once()` 注册，emit 两次只触发一次 |
| off (all) | `off(event)` 移除所有该事件的监听器 |
| off (returned) | `on()` 返回值调用即取消该监听器 |
| off (specific handler) | `off(event, handler)` 只移除指定 handler，其余不受影响 |

#### `invoke.spec.ts` — Unary RPC

| 场景 | 描述 |
|------|------|
| request-response | 基础请求-响应，验证返回值 |
| lazy context (sync/async) | `defineInvoke(() => ctx)` 延迟获取 context |
| error propagation | handler 抛异常，invoke Promise reject 同一错误对象 |
| abort/cancel | `AbortController.abort()` → invoke reject AbortError，handler 收到 abort 通知 |
| concurrent invokes | 3 个并行 invoke，互不干扰 |
| same handler once | 同一 handler 注册两次只生效一次 |
| undefine handler | `undefineInvokeHandler()` 移除单个/全部 handler |
| batch registration | `defineInvokeHandlers()` + `defineInvokes()` 批量注册和调用 |
| stream input | 请求为 `ReadableStream<number>`，handler 用 `for await` 消费，返回聚合结果 |
| abort stream input | 定时流输入（250ms/item），第4-5项之间 abort，验证已接收4项、handler 收到 AbortError、耗时在预期范围 |

#### `stream.spec.ts` — 流式 RPC

| 场景 | 描述 |
|------|------|
| server-streaming | AsyncGenerator handler，客户端 `for await` 收集所有 chunk |
| toStreamHandler | 回调风格 handler（`emit()` 推送）等价于 generator |
| concurrent streams | 3 个并行 stream invoke，每个结果独立验证 |
| error surfacing | handler 抛异常，客户端 `for await` 收到同一 Error 对象 |
| abort stream | `AbortController.abort()` → 客户端 ReadableStream error，handler 收到 abort |
| cancel stream | `stream.cancel()` → handler 收到 abort |
| abort request stream | 定时流输入 + 流式响应，中途 abort，验证已接收项和耗时 |
| request stream input | `ReadableStream<number>` 输入 → `AsyncGenerator` 输出，bidi 模式 |
| toStreamHandler + stream input | bidi + 回调风格 handler |

#### `invoke-shared.spec.ts` — InvokeEventa 结构

验证 `defineInvokeEventa()` 生成的 7 事件拥有正确的 `invokeType` 枚举值和唯一 `id`。

#### `invoke-remote-methods.spec.ts` — Function Stub 扩展

| 场景 | 描述 |
|------|------|
| function stubs | payload 中含函数，自动序列化为 stub，另一端可调用 |
| dispose | 手动清理 stub handler |
| maxFunctions | 超出函数数量限制则 reject |
| disallowed tag | 非法 tag 的忽略/抛出策略 |
| prototype pollution (6个场景) | `__proto__`、`constructor.prototype`、嵌套、数组等攻击向量全部验证 |
| auto-dispose | 超时自动清理 stub handler |
| strict mode | 畸形 stub payload 抛错 |

#### `context-extension-invoke-internal.spec.ts` — 适配器取消扩展

验证 `registerInvokeAbortEventListeners()` 注册的事件能自动 reject 所有 pending invoke。

#### `utils.spec.ts` — 工具函数

`isAsyncIterable()`、`isReadableStream()` 的正例和反例。

---

## 3. C# .NET 10 映射设计

### 3.1 总体原则

> **Eventa 仅作为协议层和 JS 参考，C# 实现必须遵循 .NET 事件驱动系统的最佳实践。**

关键差异决策：

| Eventa (TS) | C# .NET 10 | 理由 |
|-------------|-----------|------|
| `defineEventa<P>()` 返回 plain object | `record EventDefinition<TPayload>` 或 `static readonly` 实例 | C# 推荐不可变、值语义的事件标识 |
| `Eventa<P>.id` 为字符串 | `string Id` 属性，同样支持手动指定或自动生成 | 保持兼容 |
| `EventContext` 闭包实现 | `class EventContext` + 接口 `IEventContext` | C# OOP 惯例 |
| `emit(event, payload)` | `void Emit<TPayload>(EventDefinition<TPayload> event, TPayload payload)` | 泛型约束保证类型安全 |
| `on()` 返回取消函数 | 返回 `IDisposable` 订阅令牌 | .NET 资源管理惯例（`using` 语法） |
| `Promise<Res>` | `Task<TRes>` / `ValueTask<TRes>` | .NET async/await |
| `AbortSignal` / `AbortController` | `CancellationToken` / `CancellationTokenSource` | .NET 取消模型 |
| `ReadableStream<T>` | `IAsyncEnumerable<T>` 或 `Channel<T>` | .NET 异步流原语 |
| `AsyncGenerator` (yield) | `async IAsyncEnumerable<T>` (yield return) | C# 8.0+ 原生支持 |
| `vi.fn()` mock | `NSubstitute` 或 `Moq` | .NET 测试生态 |
| Vitest | xUnit + FluentAssertions | .NET 测试标准 |

### 3.2 事件定义

```csharp
// 不可变事件定义，record 提供值相等语义
public record EventDefinition<TPayload>(string Id)
{
    public EventDefinition() : this(IdGenerator.New()) { }
}

// Invoke 事件定义，关联 7 个子事件
public record InvokeEventDefinition<TRes, TReq>(string Tag)
{
    public InvokeEventDefinition() : this(IdGenerator.New()) { }

    public string SendEventId => $"{Tag}-send";
    public string SendErrorId => $"{Tag}-send-error";
    public string SendStreamEndId => $"{Tag}-send-stream-end";
    public string SendAbortId => $"{Tag}-send-abort";
    public string ReceiveEventId => $"{Tag}-receive";
    public string ReceiveErrorId => $"{Tag}-receive-error";
    public string ReceiveStreamEndId => $"{Tag}-receive-stream-end";
}

// 匹配表达式
public record MatchExpression<TPayload>(
    string Id,
    Func<EventEnvelope<TPayload>, bool> Matcher
);

// 静态工厂
public static class Eventa
{
    public static EventDefinition<TPayload> Define<TPayload>(string? id = null)
        => new(id ?? IdGenerator.New());

    public static InvokeEventDefinition<TRes, TReq> DefineInvoke<TRes, TReq>(string? tag = null)
        => new(tag ?? IdGenerator.New());
}
```

### 3.3 EventContext

```csharp
public interface IEventContext : IDisposable
{
    void Emit<TPayload>(EventDefinition<TPayload> eventDef, TPayload payload);

    IDisposable On<TPayload>(
        EventDefinition<TPayload> eventDef,
        Action<EventEnvelope<TPayload>> handler);

    IDisposable Once<TPayload>(
        EventDefinition<TPayload> eventDef,
        Action<EventEnvelope<TPayload>> handler);

    void Off<TPayload>(
        EventDefinition<TPayload> eventDef,
        Action<EventEnvelope<TPayload>>? handler = null);

    // 匹配表达式重载
    IDisposable On<TPayload>(
        MatchExpression<TPayload> match,
        Action<EventEnvelope<TPayload>> handler);
}

// 事件信封，对应 TS 中的 { ...event, body: payload }
public record EventEnvelope<TPayload>(string EventId, TPayload Body);
```

**实现要点**：

- 内部使用 `ConcurrentDictionary<string, ConcurrentBag<Delegate>>` 保证线程安全（TS 版无需考虑线程安全，C# 必须考虑）
- `On()` 返回 `IDisposable`，调用 `Dispose()` 即取消订阅
- `Once()` 内部注册后自动在首次触发时移除（同 TS）
- 匹配表达式按独立字典存储，`Emit()` 时遍历执行 matcher

### 3.4 Invoke 扩展

```csharp
public static class EventInvoke
{
    /// <summary>
    /// 创建客户端 invoke 函数（Unary RPC）
    /// </summary>
    public static Func<TReq, CancellationToken, Task<TRes>> DefineInvoke<TRes, TReq>(
        IEventContext ctx,
        InvokeEventDefinition<TRes, TReq> eventDef)
    {
        return async (req, ct) =>
        {
            var invokeId = IdGenerator.New();
            var tcs = new TaskCompletionSource<TRes>(
                TaskCreationOptions.RunContinuationsAsynchronously);

            // CancellationToken 注册
            using var ctr = ct.Register(() =>
            {
                ctx.Emit(/* sendAbort */, new AbortPayload(invokeId, ct.ToString()));
                tcs.TrySetCanceled(ct);
            });

            // 监听 receiveEvent-{invokeId}
            using var onReceive = ctx.On(receiveEvent, envelope =>
            {
                if (envelope.Body.InvokeId != invokeId) return;
                tcs.TrySetResult(envelope.Body.Content);
            });

            // 监听 receiveEventError-{invokeId}
            using var onError = ctx.On(receiveErrorEvent, envelope =>
            {
                if (envelope.Body.InvokeId != invokeId) return;
                tcs.TrySetException(envelope.Body.Error);
            });

            // 发送请求
            ctx.Emit(sendEvent, new SendPayload<TReq>(invokeId, req));

            return await tcs.Task;
        };
    }

    /// <summary>
    /// 注册服务端 invoke handler
    /// </summary>
    public static IDisposable DefineInvokeHandler<TRes, TReq>(
        IEventContext ctx,
        InvokeEventDefinition<TRes, TReq> eventDef,
        Func<TReq, CancellationToken, Task<TRes>> handler)
    {
        // 监听 sendEvent，提取 invokeId，执行 handler，通过 receiveEvent 返回结果
        // 监听 sendAbort，取消对应的 CancellationTokenSource
        // 返回 IDisposable 用于取消注册
    }
}
```

**C# 特有设计**：

- 使用 `TaskCompletionSource<TRes>` + `TaskCreationOptions.RunContinuationsAsynchronously` 避免死锁
- `CancellationToken` 替代 `AbortSignal`，语义完全对应
- Handler 签名 `Func<TReq, CancellationToken, Task<TRes>>`，handler 内可直接 `ct.ThrowIfCancellationRequested()`
- InvokeId 关联逻辑与 TS 完全一致（事件 ID 拼接）

### 3.5 Context 扩展

```csharp
// 适配器接口
public interface IEventaAdapter : IDisposable
{
    /// <summary>
    /// ctx.emit() 调用后触发，负责将事件序列化并发送到传输层
    /// </summary>
    void OnSent(string eventId, object payload, object? options = null);

    /// <summary>
    /// ctx.on()/ctx.once() 匹配到消息时触发（观测 hook）
    /// </summary>
    void OnReceived(string eventId, object payload);
}

// 带适配器的 context 工厂
public static IEventContext CreateContext(IEventaAdapter? adapter = null)
{
    return new EventContext(adapter);
}
```

**Context 扩展点**（对应 TS `extensions`）：

```csharp
public interface IEventContext
{
    // ... 基础方法 ...

    /// <summary>
    /// 扩展属性包，适配器可存储内部状态
    /// </summary>
    IDictionary<string, object> Extensions { get; }
}

// Invoke 内部配置（适配器 abort 扩展）
public class InvokeInternalConfig
{
    public List<EventDefinition<object>> AbortOnEvents { get; } = new();
    public Func<EventEnvelope<object>, Exception?>? MapAbortError { get; set; }
}

public static class InvokeExtensions
{
    public static void RegisterAbortEvent(
        this IEventContext ctx,
        EventDefinition<object> fatalEvent)
    {
        // 存入 ctx.Extensions["__internal.invoke"]
    }
}
```

### 3.6 Emit 扩展

TS 中 `EmitOptions` 是泛型参数，适配器可注入额外选项。C# 对应设计：

```csharp
// 基础 emit
void Emit<TPayload>(EventDefinition<TPayload> eventDef, TPayload payload);

// 带选项的 emit（适配器扩展）
void Emit<TPayload, TOptions>(
    EventDefinition<TPayload> eventDef,
    TPayload payload,
    TOptions options) where TOptions : class;

// 使用示例（SignalR 适配器）
ctx.Emit(moveEvent, new MoveData(100, 200), new SignalROptions
{
    GroupName = "room-1",
    ExcludeConnectionId = connectionId
});
```

### 3.7 Stream 扩展

```csharp
public static class EventStream
{
    /// <summary>
    /// 服务端流式响应（Server-Streaming）
    /// </summary>
    public static IAsyncEnumerable<TRes> DefineStreamInvoke<TRes, TReq>(
        IEventContext ctx,
        InvokeEventDefinition<TRes, TReq> eventDef,
        TReq request,
        CancellationToken ct = default)
    {
        // 返回 IAsyncEnumerable<TRes>，内部使用 Channel<TRes> 桥接
        // 监听 receiveEvent → channel.Writer.TryWrite()
        // 监听 receiveEventStreamEnd → channel.Writer.Complete()
        // 监听 receiveEventError → channel.Writer.Complete(exception)
    }

    /// <summary>
    /// 注册流式 handler（使用 async yield）
    /// </summary>
    public static IDisposable DefineStreamInvokeHandler<TRes, TReq>(
        IEventContext ctx,
        InvokeEventDefinition<TRes, TReq> eventDef,
        Func<TReq, CancellationToken, IAsyncEnumerable<TRes>> handler)
    {
        // 每个 yield return 的值 → emit receiveEvent
        // 枚举结束 → emit receiveEventStreamEnd
        // 异常 → emit receiveEventError
    }
}
```

**C# 特有亮点**：

- `IAsyncEnumerable<T>` 是 C# 8.0+ 的一等公民，完美对应 TS 的 `AsyncGenerator`
- `Channel<T>`（`System.Threading.Channels`）作为内部缓冲区，性能优于手动 Task 链
- `CancellationToken` 通过 `[EnumeratorCancellation]` 特性与 `await foreach` 自然集成
- 双向流可使用 `IAsyncEnumerable<TReq>` 输入 + `IAsyncEnumerable<TRes>` 输出

```csharp
// 双向流 handler 签名
Func<IAsyncEnumerable<TReq>, CancellationToken, IAsyncEnumerable<TRes>> bidiHandler;

// 使用
await foreach (var response in StreamInvoke(inputStream, ct))
{
    Console.WriteLine(response);
}
```

### 3.8 `toStreamHandler` 等价设计

TS 中 `toStreamHandler` 将回调风格（`emit(data)`）转换为 `AsyncGenerator`。C# 等价：

```csharp
public static Func<TReq, CancellationToken, IAsyncEnumerable<TRes>>
    ToStreamHandler<TReq, TRes>(
        Func<TReq, Action<TRes>, CancellationToken, Task> callback)
{
    return (req, ct) => StreamFromCallback(req, callback, ct);
}

private static async IAsyncEnumerable<TRes> StreamFromCallback<TReq, TRes>(
    TReq req,
    Func<TReq, Action<TRes>, CancellationToken, Task> callback,
    [EnumeratorCancellation] CancellationToken ct)
{
    var channel = Channel.CreateUnbounded<TRes>();

    _ = Task.Run(async () =>
    {
        try
        {
            await callback(req, item => channel.Writer.TryWrite(item), ct);
            channel.Writer.Complete();
        }
        catch (Exception ex)
        {
            channel.Writer.Complete(ex);
        }
    }, ct);

    await foreach (var item in channel.Reader.ReadAllAsync(ct))
    {
        yield return item;
    }
}
```

---

## 4. 适配器抽象层

### 4.1 已有 Eventa 适配器及 C# 等价方案

| TS 适配器 | C# .NET 等价传输层 | 实现难度 |
|----------|-------------------|---------|
| EventTarget | 内存直连（`EventContext` 本身） | ⭐ 低 |
| EventEmitter | 内存直连 / 自定义 `IObservable<T>` | ⭐ 低 |
| BroadcastChannel | `System.Threading.Channels` / 进程内管道 | ⭐ 低 |
| WebSocket (client) | `System.Net.WebSockets.ClientWebSocket` | ⭐⭐ 中 |
| WebSocket (H3 server) | ASP.NET Core WebSocket middleware / SignalR | ⭐⭐ 中 |
| Electron Main/Renderer | 不适用（Electron 是 JS 生态特有） | N/A |
| Web Worker | 不适用 | N/A |
| Worker Threads | `System.Threading` / Channel 跨线程 | ⭐⭐ 中 |

### 4.2 推荐首批适配器

1. **InMemory**（`EventContext` 直连，测试用）— 优先级 P0
2. **Channel（跨线程/进程内管道）** — 优先级 P0
3. **WebSocket**（`ClientWebSocket` + ASP.NET Core） — 优先级 P1
4. **SignalR** — 优先级 P2（SignalR 本身已有 streaming hub，但 Eventa 抽象层统一了 API）
5. **gRPC**（`Grpc.Net.Client` / `Grpc.AspNetCore`） — 优先级 P2

### 4.3 适配器 Hooks 映射

```csharp
public interface IEventaAdapter : IDisposable
{
    void OnSent(string eventId, object payload, object? options = null);
    void OnReceived(string eventId, object payload);
}

// WebSocket 适配器示例
public class WebSocketAdapter : IEventaAdapter
{
    private readonly WebSocket _ws;

    public void OnSent(string eventId, object payload, object? options = null)
    {
        var json = JsonSerializer.Serialize(new { eventId, payload });
        var bytes = Encoding.UTF8.GetBytes(json);
        _ws.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
    }

    public void OnReceived(string eventId, object payload)
    {
        // 观测 hook，可用于日志/指标
    }

    // 构造函数中启动接收循环，收到消息后调用 emit
}
```

---

## 5. 测试策略映射

### 5.1 框架对照

| TS (Vitest) | C# (.NET 10) |
|-------------|-------------|
| `describe` / `it` | xUnit `[Fact]` / `[Theory]` + 嵌套类 |
| `vi.fn()` | NSubstitute `Substitute.For<T>()` 或 Moq `Mock<T>()` |
| `expect(x).toBe(y)` | FluentAssertions `x.Should().Be(y)` |
| `expect(promise).rejects.toThrowError()` | `await act.Should().ThrowAsync<Exception>()` |
| `expectTypeOf<T>()` | 编译期测试（C# 强类型语言天然满足） |
| `await sleep(ms)` | `await Task.Delay(ms)` |

### 5.2 每个测试场景的 C# 等价写法概要

#### EventContext 基础测试

```csharp
public class EventContextTests
{
    [Fact]
    public void Should_RegisterAndEmit()
    {
        var ctx = EventContext.Create();
        var testEvent = Eventa.Define<TestData>();
        var received = new List<EventEnvelope<TestData>>();

        using var sub = ctx.On(testEvent, e => received.Add(e));
        ctx.Emit(testEvent, new TestData("test"));

        received.Should().ContainSingle()
            .Which.Body.Value.Should().Be("test");
    }

    [Fact]
    public void Should_HandleOnce()
    {
        var ctx = EventContext.Create();
        var testEvent = Eventa.Define<string>();
        var count = 0;

        using var sub = ctx.Once(testEvent, _ => count++);
        ctx.Emit(testEvent, "a");
        ctx.Emit(testEvent, "b");

        count.Should().Be(1);
    }

    [Fact]
    public void Should_RemoveListenerViaDispose()
    {
        var ctx = EventContext.Create();
        var testEvent = Eventa.Define<string>();
        var count = 0;

        var sub = ctx.On(testEvent, _ => count++);
        sub.Dispose();
        ctx.Emit(testEvent, "test");

        count.Should().Be(0);
    }
}
```

#### Invoke 测试

```csharp
public class InvokeTests
{
    [Fact]
    public async Task Should_HandleRequestResponse()
    {
        var ctx = EventContext.Create();
        var events = Eventa.DefineInvoke<UserResponse, UserRequest>();

        using var handler = EventInvoke.DefineInvokeHandler(ctx, events,
            (req, ct) => Task.FromResult(new UserResponse($"user-{req.Name}")));

        var invoke = EventInvoke.DefineInvoke(ctx, events);
        var result = await invoke(new UserRequest("alice"), CancellationToken.None);

        result.Id.Should().Be("user-alice");
    }

    [Fact]
    public async Task Should_PropagateHandlerErrors()
    {
        var ctx = EventContext.Create();
        var events = Eventa.DefineInvoke<string, string>();

        using var handler = EventInvoke.DefineInvokeHandler<string, string>(ctx, events,
            (_, _) => throw new InvalidOperationException("handler failed"));

        var invoke = EventInvoke.DefineInvoke(ctx, events);

        var act = () => invoke("test", CancellationToken.None);
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("handler failed");
    }

    [Fact]
    public async Task Should_CancelViaToken()
    {
        var ctx = EventContext.Create();
        var events = Eventa.DefineInvoke<string, int>();
        var cts = new CancellationTokenSource();

        using var handler = EventInvoke.DefineInvokeHandler(ctx, events,
            async (_, ct) =>
            {
                await Task.Delay(Timeout.Infinite, ct); // 等待取消
                return "ok";
            });

        var invoke = EventInvoke.DefineInvoke(ctx, events);
        var task = invoke(42, cts.Token);

        cts.Cancel();

        var act = () => task;
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task Should_HandleConcurrentInvokes()
    {
        var ctx = EventContext.Create();
        var events = Eventa.DefineInvoke<int, int>();

        using var handler = EventInvoke.DefineInvokeHandler(ctx, events,
            (val, _) => Task.FromResult(val * 2));

        var invoke = EventInvoke.DefineInvoke(ctx, events);

        var results = await Task.WhenAll(
            invoke(10, default),
            invoke(20, default),
            invoke(50, default));

        results.Should().BeEquivalentTo(new[] { 20, 40, 100 });
    }
}
```

#### Stream 测试

```csharp
public class StreamTests
{
    [Fact]
    public async Task Should_HandleServerStreaming()
    {
        var ctx = EventContext.Create();
        var events = Eventa.DefineInvoke<ProgressOrResult, JobRequest>();

        using var handler = EventStream.DefineStreamInvokeHandler(ctx, events,
            async (req, ct) => ServerStreamImpl(req, ct));

        var results = new List<ProgressOrResult>();
        await foreach (var item in EventStream.DefineStreamInvoke(ctx, events,
            new JobRequest("alice"), default))
        {
            results.Add(item);
        }

        results.Should().HaveCount(6); // 5 progress + 1 result
    }

    private static async IAsyncEnumerable<ProgressOrResult> ServerStreamImpl(
        JobRequest req,
        [EnumeratorCancellation] CancellationToken ct)
    {
        for (var i = 1; i <= 5; i++)
        {
            yield return new ProgressOrResult.Progress(i * 20);
        }
        yield return new ProgressOrResult.Result(true);
    }
}
```

---

## 6. 推荐项目结构

```
Eventa.sln
├── src/
│   ├── Eventa.Core/                     # 核心库
│   │   ├── EventDefinition.cs           # record EventDefinition<T>
│   │   ├── EventContext.cs              # IEventContext 实现
│   │   ├── InvokeEventDefinition.cs     # InvokeEventa 7 事件对
│   │   ├── EventInvoke.cs              # defineInvoke / defineInvokeHandler
│   │   ├── EventStream.cs             # defineStreamInvoke / defineStreamInvokeHandler
│   │   ├── MatchExpression.cs          # matchBy / and / or
│   │   ├── IEventaAdapter.cs           # 适配器接口
│   │   └── IdGenerator.cs              # nanoid 等价
│   │
│   ├── Eventa.Adapters.WebSocket/       # WebSocket 适配器
│   ├── Eventa.Adapters.SignalR/         # SignalR 适配器
│   ├── Eventa.Adapters.Channels/        # System.Threading.Channels 适配器
│   └── Eventa.Adapters.Grpc/           # gRPC 适配器
│
├── tests/
│   ├── Eventa.Core.Tests/              # 核心单元测试
│   │   ├── EventContextTests.cs
│   │   ├── InvokeTests.cs
│   │   ├── StreamTests.cs
│   │   └── MatchExpressionTests.cs
│   │
│   └── Eventa.Adapters.Tests/          # 适配器集成测试
│
└── samples/
    ├── Eventa.Sample.Console/           # 控制台示例
    └── Eventa.Sample.WebApi/           # ASP.NET Core 示例
```

**NuGet 包划分**：

| 包名 | 内容 | 依赖 |
|------|------|------|
| `Eventa.Core` | 事件定义、Context、Invoke、Stream | 无外部依赖 |
| `Eventa.Adapters.WebSocket` | WebSocket 适配器 | `Eventa.Core` |
| `Eventa.Adapters.SignalR` | SignalR 适配器 | `Eventa.Core` + `Microsoft.AspNetCore.SignalR` |
| `Eventa.Adapters.Grpc` | gRPC 适配器 | `Eventa.Core` + `Grpc.Net.Client` |

---

## 7. 风险与差异

### 7.1 无直接对应的 TS 特性

| TS 特性 | 影响 | C# 替代方案 |
|---------|------|------------|
| 条件类型 (`T extends X ? Y : Z`) | Invoke 函数签名中根据 Req 是否 undefined 决定参数可选 | 提供多个重载（`Invoke()` 和 `Invoke(TReq req)`） |
| `Transferable` / Structured Clone | `withTransfer()` 不适用 | 不需要：C# 跨进程通信使用序列化，无 Structured Clone 概念 |
| glob 匹配 (`picomatch`) | `matchBy('pattern*')` | 使用 `Microsoft.Extensions.FileSystemGlobbing` 或正则表达式 |
| Function Stub 序列化 | `withRemoteMethods()` 将函数序列化为标记 | **低优先级**：C# 中 delegate 不可序列化，需要不同设计（如注册命名服务）。建议作为 v2 特性 |

### 7.2 必须额外考虑的 C# 问题

| 问题 | 说明 | 建议 |
|------|------|------|
| **线程安全** | TS 单线程无需同步；C# 多线程环境必须考虑 | 内部使用 `ConcurrentDictionary` + `lock`/`ReaderWriterLockSlim` |
| **内存泄漏** | TS 依赖 GC 和闭包；C# 中事件订阅是强引用 | `On()` 返回 `IDisposable`，强烈建议 `using` 语法 |
| **异常传播** | TS catch 所有类型；C# 区分 `Exception`/`OperationCanceledException` | Handler 异常包装为 `EventaInvokeException`，取消使用 `OperationCanceledException` |
| **序列化** | TS 使用 JSON/Structured Clone；C# 需要显式选择 | 默认 `System.Text.Json`，适配器可插入自定义 `IEventaSerializer` |
| **性能** | `Channel<T>` 高吞吐替代 TS `ReadableStream` | 使用 `BoundedChannelOptions` 控制背压 |

### 7.3 不迁移的部分

- **Function Stub (`withRemoteMethods`)** — 标记为低优先级。TS 中利用函数是一等公民的特性实现自动序列化，C# 中 delegate 不可跨进程传递。如需类似功能，建议使用「命名服务注册」模式
- **`withTransfer`** — Structured Clone Transfer 是浏览器特有概念，C# 不需要
- **Electron 适配器** — 纯 JS 生态

---

## 8. 实施路线图建议

### Phase 1 — 核心库（Eventa.Core）

1. `EventDefinition<T>` / `InvokeEventDefinition<TRes, TReq>` record 定义
2. `EventContext`（内存直连，emit/on/once/off）
3. `DefineInvoke` / `DefineInvokeHandler`（含 CancellationToken、流式输入）
4. `DefineStreamInvoke` / `DefineStreamInvokeHandler`（IAsyncEnumerable）
5. `MatchExpression`、`And()`、`Or()`
6. 完整单元测试（映射自 TS 所有 spec）

### Phase 2 — 适配器

1. WebSocket 适配器（ClientWebSocket + ASP.NET Core middleware）
2. Channel 适配器（跨线程管道）
3. SignalR 适配器

### Phase 3 — 高级特性

1. Context 扩展（`extensions` 字典 + abort event 注册）
2. `toStreamHandler` 辅助
3. 全局元数据 (`metadata` / `invokeMetadata`)
4. 序列化器抽象 (`IEventaSerializer`)

---

## 9. 结论

**迁移完全可行**。Eventa 的核心设计（事件定义 → Context 发布/订阅 → 7 事件协议 Invoke/Stream → 适配器 hooks）在 C# .NET 10 中均有惯用且更强大的对应方案：

- `record` 替代 plain object 事件定义，天然不可变 + 值相等
- `IDisposable` 替代 `() => void` 取消函数，与 `using` 语法集成
- `Task<T>` / `ValueTask<T>` 替代 `Promise<T>`，async/await 完全对应
- `CancellationToken` 替代 `AbortSignal`，更成熟的取消模型
- `IAsyncEnumerable<T>` 替代 `AsyncGenerator` / `ReadableStream`，一等公民支持
- `Channel<T>` 提供高性能异步缓冲，替代 TS 中手动管理的 `ReadableStream`
- 线程安全可通过 `ConcurrentDictionary` / `Channel` / `lock` 自然解决

核心库预计代码量约 800-1200 行（不含测试），与 TS 版体量相当。
