namespace Eventa.Tests;

public class EventDefinitionTests
{
    [Fact]
    public void Constructor_PreservesSuppliedId()
    {
        var definition = new EventDefinition<TestPayload>("user-created");

        Assert.Equal("user-created", definition.Id);
    }

    private sealed record TestPayload(string Value);
}
