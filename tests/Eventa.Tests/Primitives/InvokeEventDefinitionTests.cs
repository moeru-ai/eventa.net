namespace Eventa.Tests;

public class InvokeEventDefinitionTests
{
    [Fact]
    public void Constructor_ExposesExpectedEventIdsFromTag()
    {
        var definition = new InvokeEventDefinition<TestResponse, TestRequest>("users");

        Assert.Equal("users", definition.Tag);
        Assert.Equal("users-send", definition.SendEventId);
        Assert.Equal("users-send-error", definition.SendErrorId);
        Assert.Equal("users-send-stream-end", definition.SendStreamEndId);
        Assert.Equal("users-send-abort", definition.SendAbortId);
        Assert.Equal("users-receive", definition.ReceiveEventId);
        Assert.Equal("users-receive-error", definition.ReceiveErrorId);
        Assert.Equal("users-receive-stream-end", definition.ReceiveStreamEndId);
    }

    private sealed record TestRequest(string Name);

    private sealed record TestResponse(string Id);
}
