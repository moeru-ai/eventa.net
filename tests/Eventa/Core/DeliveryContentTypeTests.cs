namespace Eventa.Tests;

public sealed class DeliveryContentTypeTests
{
    [Theory]
    [InlineData("application/example")]
    [InlineData("Application/Vnd.Example+Binary; Version=1; PROTOCOL=\"invoke\"")]
    [InlineData("text/plain; charset=utf-8")]
    [InlineData("application/example ;\tkey=\"a;b=c\\\"d\\\\e\"; empty=\"\"")]
    [InlineData("application/example;; ;key=value;")]
    [InlineData("application/example; ")]
    [InlineData("application/example;\t; \t")]
    [InlineData("application/example; key=\"value \" \t; \t")]
    [InlineData("application/example; text=\"a\tb é\"")]
    [InlineData("x!#$%&'*+-.^_`|~/example")]
    public void Construction_WithValidMediaType_PreservesExactInput(string value)
    {
        Eventa eventa = new("event");
        Assert.Same(value, Delivery.Create(value, eventa).contentType);
        Assert.Same(value, Delivery.Restore(new DeliveryInput(value, "id", 1, eventa)).contentType);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(" text/plain")]
    [InlineData("\ttext/plain")]
    [InlineData("text/plain ")]
    [InlineData("text/plain\t")]
    [InlineData("\t application/example;\t; \t")]
    [InlineData("application")]
    [InlineData("/example")]
    [InlineData("application/")]
    [InlineData("application /example")]
    [InlineData("application/ example")]
    [InlineData("application/example/extra")]
    [InlineData("application/example, text/plain")]
    [InlineData("application/example; flag")]
    [InlineData("application/example; key=")]
    [InlineData("application/example; =value")]
    [InlineData("application/example; key =value")]
    [InlineData("application/example; key= value")]
    [InlineData("application/example; key=value\t")]
    [InlineData("application/example; key=\"value\" ")]
    [InlineData("application/example; key=\"unterminated")]
    [InlineData("application/example; key=\"escape\\")]
    [InlineData("application/example; key=\"escape\\\"")]
    [InlineData("application/example; key=\"x\"junk")]
    [InlineData("application/example; key=\"a\r\nb\"")]
    [InlineData("application/example\r\nInjected: yes")]
    [InlineData("application/example; key=\"\u007f\"")]
    [InlineData("application/example; key=\"\\\u0001\"")]
    [InlineData("application/example; key=\"中\"")]
    public void Construction_WithInvalidMediaType_RejectsInCreateAndRestore(string value)
    {
        Eventa eventa = new("event");
        Assert.Throws<ArgumentException>(() => Delivery.Create(value, eventa));
        Assert.Throws<ArgumentException>(() => Delivery.Restore(new DeliveryInput(value, "id", 1, eventa)));
    }

    [Theory]
    [InlineData("Application/Example; VERSION=1; PrOtOcOl=\"invoke\"")]
    [InlineData("application/example;protocol=invoke;version=1")]
    public void Lookup_WithEquivalentMediaTypes_MatchesTypeAndParameters(string value)
    {
        Assert.True(DeliveryContentType.IsMediaType(value, "application/EXAMPLE"));
        Assert.False(DeliveryContentType.IsMediaType(value, "application/other"));
        Assert.Equal("invoke", DeliveryContentType.GetParameter(value, "PROTOCOL"));
        Assert.Equal("1", DeliveryContentType.GetParameter(value, "version"));
        Assert.Null(DeliveryContentType.GetParameter(value, "missing"));
    }

    [Fact]
    public void Lookup_WithEscapedAndMixedCaseValues_DecodesOnlyNamedParameters()
    {
        const string value = "application/example; xprotocol=invoke; text=\"protocol=invoke\"; a=\"A\\\"B\\\\C\"";
        Assert.Null(DeliveryContentType.GetParameter(value, "protocol"));
        Assert.Equal("A\"B\\C", DeliveryContentType.GetParameter(value, "a"));
        Assert.Equal("Invoke", DeliveryContentType.GetParameter("application/example;protocol=Invoke", "protocol"));
        Assert.Null(DeliveryContentType.GetParameter("invalid;protocol=invoke", "protocol"));
    }

    [Fact]
    public void Lookup_WithEmptySegmentsAndRepeatedNames_ReturnsFirstDecodedValue()
    {
        const string value = "application/example; ; Name=\";A / B = C;\\é\\\"D\";; NAME=second; empty=\"\";";

        Assert.Equal(";A / B = C;é\"D", DeliveryContentType.GetParameter(value, "name"));
        Assert.Equal(string.Empty, DeliveryContentType.GetParameter(value, "empty"));
    }
}
