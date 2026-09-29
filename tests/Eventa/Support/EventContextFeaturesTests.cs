namespace Eventa.Tests;

public class EventContextFeaturesTests
{
    [Fact]
    public void GetOrCreateFeature_ReturnsTheSameTypedInstanceForTheSameKey()
    {
        var context = new EventContext();

        var first = context.GetOrCreateFeature<TestFeature>("feature");
        first.Value = 42;

        var second = context.GetOrCreateFeature<TestFeature>("feature");

        Assert.Same(first, second);
        Assert.Same(first, context.Extensions["feature"]);
        Assert.Equal(42, second.Value);
    }

    [Fact]
    public void GetOrCreateFeature_WhenStoredValueHasDifferentType_ReplacesIt()
    {
        var context = new EventContext
        {
            Extensions =
            {
                ["feature"] = "wrong shape",
            },
        };

        var feature = context.GetOrCreateFeature<TestFeature>("feature");

        Assert.Same(feature, context.Extensions["feature"]);
    }

    [Fact]
    public void TryGetFeature_ReturnsFalseWhenTheKeyIsMissingOrHasADifferentType()
    {
        var context = new EventContext
        {
            Extensions =
            {
                ["feature"] = "wrong shape",
            },
        };

        Assert.False(context.TryGetFeature<TestFeature>("missing", out var missing));
        Assert.Null(missing);

        Assert.False(context.TryGetFeature<TestFeature>("feature", out var mismatched));
        Assert.Null(mismatched);
    }

    private sealed class TestFeature
    {
        public int Value { get; set; }
    }
}
