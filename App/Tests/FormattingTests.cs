using FluentAssertions;
using NUnit.Framework;

namespace Tests;

public static class FormattingTests {
  [Test]
  public static void TestTimestampHumanise() {
    120.Seconds().HumanizeShort().Should().Be("2m 0s");
    0.Seconds().HumanizeShort().Should().Be("0s");
    1.6.Seconds().HumanizeShort().Should().Be("1.6s");
    0.12.Seconds().HumanizeShort().Should().Be("120ms");
    new TimeSpan(days: 1, hours: 2, minutes: 3, seconds: 4).HumanizeShort().Should().Be("1d 2h");
  }
}