using System;
using FluentAssertions;
using Mutuo.Etl.Blob;
using Newtonsoft.Json;
using NUnit.Framework;
using SysExtensions.Serialization;

namespace Tests {
  public class SerializationTests {
    [Test]
    public void TestSerializeRecord() {
      var file = new FileListItem("my/path", new DateTimeOffset(2020, 05, 04, 10, 2, 0, 0, TimeSpan.Zero), 500);
      file.ToJson(JsonExtensions.DefaultSettings(Formatting.None)).Should().Be("{\"path\":\"my/path\",\"modified\":\"2020-05-04T10:02:00+00:00\",\"bytes\":500}");
      file.ToJson().ToObject<FileListItem>().Should().BeEquivalentTo(file);
    }
  }
}