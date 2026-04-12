namespace Eventa.Tests;

public class EventaDefinitionTests
{
    [Fact]
    public void EventDefinition_ConstructorPreservesSuppliedId()
    {
        var definition = new EventDefinition<TestPayload>("user-created");

        Assert.Equal("user-created", definition.Id);
    }

    [Fact]
    public void Eventa_Define_ReturnsEventWithSuppliedId()
    {
        var definition = Eventa.Define<TestPayload>("user-created");

        Assert.Equal("user-created", definition.Id);
    }

    [Fact]
    public void Eventa_Define_ReturnsUniqueIdsWhenCalledTwice()
    {
        var first = Eventa.Define<TestPayload>();
        var second = Eventa.Define<TestPayload>();

        Assert.NotEmpty(first.Id);
        Assert.NotEmpty(second.Id);
        Assert.NotEqual(first.Id, second.Id);
    }

    [Fact]
    public void InvokeEventDefinition_ExposesExpectedEventIdsFromTag()
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

    [Fact]
    public void Eventa_DefineInvoke_ReturnsDefinitionWithRequestedTag()
    {
        var definition = Eventa.DefineInvoke<TestResponse, TestRequest>("users");

        Assert.Equal("users", definition.Tag);
        Assert.Equal("users-send", definition.SendEventId);
        Assert.Equal("users-receive", definition.ReceiveEventId);
    }

    [Fact]
    public void Eventa_DefineInvoke_ReturnsUniqueTagsWhenCalledTwice()
    {
        var first = Eventa.DefineInvoke<TestResponse, TestRequest>();
        var second = Eventa.DefineInvoke<TestResponse, TestRequest>();

        Assert.NotEmpty(first.Tag);
        Assert.NotEmpty(second.Tag);
        Assert.NotEqual(first.Tag, second.Tag);
    }

    private sealed record TestPayload(string Value);

    private sealed record TestRequest(string Name);

    private sealed record TestResponse(string Id);
}
