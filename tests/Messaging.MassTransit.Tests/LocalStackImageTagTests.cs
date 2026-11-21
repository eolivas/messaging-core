namespace Messaging.MassTransit.Tests;

public sealed class LocalStackImageTagTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void Resolve_NullOrEmpty_ReturnsDefault(string? value) =>
        Assert.Equal("4.14.0", LocalStackImageTag.Resolve(value));

    [Fact]
    public void Resolve_PatchVersion_ReturnsOverride() =>
        Assert.Equal("4.9.2", LocalStackImageTag.Resolve("4.9.2"));

    [Theory]
    [InlineData("latest")]
    [InlineData("4")]
    [InlineData("4.14")]
    [InlineData("4.14.0-beta")]
    [InlineData(" 4.14.0")]
    [InlineData("4.14.0\n")]
    public void Resolve_FloatingOrPartial_Throws(string value) =>
        Assert.Throws<ArgumentException>(() => LocalStackImageTag.Resolve(value));
}
