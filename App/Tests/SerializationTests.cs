using FluentAssertions;
using Mutuo.Etl.Blob;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Tests;

public class SerializationTests {
  [Test]
  public void TestSerializeRecord() {
    var file = new FileListItem("my/path", new DateTimeOffset(year: 2020, month: 05, day: 04, hour: 10, minute: 2, second: 0, millisecond: 0, TimeSpan.Zero),
      Bytes: 500);
    file.ToJson(JsonExtensions.DefaultSettings(Formatting.None)).Should()
      .Be("{\"path\":\"my/path\",\"modified\":\"2020-05-04T10:02:00+00:00\",\"bytes\":500}");
    file.ToJson().ToObject<FileListItem>().Should().BeEquivalentTo(file);
  }
}