namespace Eventa.Tests;

public class DefinitionConstructionTests
{
    [Fact]
    public void EventDefinition_ConstructorUsesSuppliedId()
    {
        var definition = new EventDefinition<TestPayload>("user-created");

        Assert.Equal("user-created", definition.Id);
    }

    [Fact]
    public void EventDefinition_DefaultConstructorUsesUniqueIds()
    {
        var first = new EventDefinition<TestPayload>();
        var second = new EventDefinition<TestPayload>();

        Assert.NotEmpty(first.Id);
        Assert.NotEmpty(second.Id);
        Assert.NotEqual(first.Id, second.Id);
    }

    [Fact]
    public void InvokeEventDefinition_ConstructorUsesRequestedTag()
    {
        var definition = new InvokeEventDefinition<TestResponse, TestRequest>("users");

        Assert.Equal("users", definition.Tag);
        Assert.Equal("users-send", definition.SendEventId);
        Assert.Equal("users-receive", definition.ReceiveEventId);
    }

    [Fact]
    public void InvokeEventDefinition_DefaultConstructorUsesUniqueTags()
    {
        var first = new InvokeEventDefinition<TestResponse, TestRequest>();
        var second = new InvokeEventDefinition<TestResponse, TestRequest>();

        Assert.NotEmpty(first.Tag);
        Assert.NotEmpty(second.Tag);
        Assert.NotEqual(first.Tag, second.Tag);
    }

    private sealed record TestPayload(string Value);

    private sealed record TestRequest(string Name);

    private sealed record TestResponse(string Id);
}
