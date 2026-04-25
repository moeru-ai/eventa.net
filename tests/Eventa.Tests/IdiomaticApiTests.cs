using System.Runtime.CompilerServices;

namespace Eventa.Tests;

public class IdiomaticApiTests
{
    [Fact]
    public async Task InvokeAsync_UsesConstructorDefinitionsAndContextRegistration()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<string, string>("echo");

        using var _ = context.RegisterInvokeHandler(
            definition,
            (string request, CancellationToken _) => Task.FromResult($"handled:{request}"));

        var result = await context.InvokeAsync(definition, "request", CancellationToken.None);

        Assert.Equal("handled:request", result);
    }

    [Fact]
    public void SubscribeOnce_And_Unsubscribe_UseContextSubscriptionNames()
    {
        var context = new EventContext();
        var definition = new EventDefinition<string>("message");
        var received = new List<string>();

        using var _ = context.SubscribeOnce(definition, envelope => received.Add($"once:{envelope.Body}"));
        using var __ = context.Subscribe(definition, envelope => received.Add($"always:{envelope.Body}"));

        context.Emit(definition, "first");
        context.Unsubscribe(definition);
        context.Emit(definition, "second");

        Assert.Equal(["always:first", "once:first"], received.Order());
    }

    [Fact]
    public async Task InvokeStreamAsync_UsesContextStreamRegistration()
    {
        var context = new EventContext();
        var definition = new InvokeEventDefinition<int, int>("count");

        using var _ = context.RegisterStreamHandler(definition, CountAsync);

        var results = new List<int>();
        await foreach (var value in context.InvokeStreamAsync(definition, 3, CancellationToken.None))
        {
            results.Add(value);
        }

        Assert.Equal([1, 2, 3], results);

        static async IAsyncEnumerable<int> CountAsync(
            int request,
            [EnumeratorCancellation] CancellationToken _)
        {
            for (var value = 1; value <= request; value++)
            {
                await Task.Yield();
                yield return value;
            }
        }
    }
}
