namespace Eventa.Tests;

public class IdGeneratorTests
{
    [Fact]
    public void IdGenerator_New_Returns16CharactersByDefault()
    {
        var id = IdGenerator.New();

        Assert.Equal(16, id.Length);
    }

    [Fact]
    public void IdGenerator_New_ReturnsRequestedLength()
    {
        var id = IdGenerator.New(24);

        Assert.Equal(24, id.Length);
    }

    [Fact]
    public void IdGenerator_New_UsesOnlyAlphanumericCharacters()
    {
        var id = IdGenerator.New(128);

        Assert.All(id, static ch => Assert.Matches("[0-9A-Za-z]", ch.ToString()));
    }

    [Fact]
    public void IdGenerator_New_ReturnsNonConstantValues()
    {
        var first = IdGenerator.New();
        var second = IdGenerator.New();

        Assert.NotEqual(first, second);
    }
}
