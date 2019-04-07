using System.ComponentModel;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;

namespace SysExtensions.Security {
    /// <summary>
    ///   Credentials for a user (in the format name:secret).
    ///   Be careful not to serialize this. it is not encrypted
    /// </summary>
    [TypeConverter(typeof(StringConverter<NameSecret>))]
  public sealed class NameSecret : IStringConvertableWithPattern {
    public NameSecret() { }

    public NameSecret(string name, string secret) {
      Name = name;
      Secret = secret;
    }

    public string Name { get; set; }
    public string Secret { get; set; }

    public string StringValue {
      get => $"{Name}:{Secret}";
      set {
        var tokens = value.UnJoin(':', '\\').ToQueue();
        Name = tokens.TryDequeue();
        Secret = tokens.TryDequeue();
      }
    }

    public string Pattern => @"([^:\n]+):([^:\n]+)";

    public override string ToString() => StringValue;
  }
}