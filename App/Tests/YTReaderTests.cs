using System;
using FluentAssertions;
using Humanizer;
using NUnit.Framework;
using SysExtensions.Text;

namespace Tests {
  public static class FormattingTests {
    [Test]
    public static void TestTimestampHumanise() {
      120.Seconds().HumanizeShort().Should().Be("2m 0s");
      0.Seconds().HumanizeShort().Should().Be("0s");
      12.Seconds().HumanizeShort().Should().Be("12s");
      TimeSpan.FromMilliseconds(2040).HumanizeShort().Should().Be("2s");
      new TimeSpan(1, 2, 3, 4).HumanizeShort().Should().Be("1d 2h");
      new TimeSpan(1, 2, 3, 4).HumanizeShort(4).Should().Be("1d 2h 3m 4s");
    }
  }
}