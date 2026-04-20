namespace Eventa.Tests;

public class EventaTests
{
    [Fact]
    public void Define_ReturnsEventWithSuppliedId()
    {
        var definition = Eventa.Define<TestPayload>("user-created");

        Assert.Equal("user-created", definition.Id);
    }

    [Fact]
    public void Define_ReturnsUniqueIdsWhenCalledTwice()
    {
        var first = Eventa.Define<TestPayload>();
        var second = Eventa.Define<TestPayload>();

        Assert.NotEmpty(first.Id);
        Assert.NotEmpty(second.Id);
        Assert.NotEqual(first.Id, second.Id);
    }

    [Fact]
    public void DefineInvoke_ReturnsDefinitionWithRequestedTag()
    {
        var definition = Eventa.DefineInvoke<TestResponse, TestRequest>("users");

        Assert.Equal("users", definition.Tag);
        Assert.Equal("users-send", definition.SendEventId);
        Assert.Equal("users-receive", definition.ReceiveEventId);
    }

    [Fact]
    public void DefineInvoke_ReturnsUniqueTagsWhenCalledTwice()
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
