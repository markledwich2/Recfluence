using System.ComponentModel;
using System.Security;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;

namespace SysExtensions.Security;

/// <summary>Credentials for a user (in the format name:secret). Be careful not to serialize this. it is not encrypted</summary>
[TypeConverter(typeof(StringConverter<NameSecret>))]
public sealed class NameSecret : IStringConvertableWithPattern {
  public NameSecret() { }

  public NameSecret(string encodedValue) {
    var (name, secret) = Parse(encodedValue);
    Name = name;
    Secret = secret;
  }

  public NameSecret(string name, string secret) {
    Name = name;
    Secret = secret;
  }

  public string Name   { get; set; }
  public string Secret { get; set; }

  public string StringValue {
    get => $"{Name}:{Secret}";
    set {
      var (name, secret) = Parse(value);
      Name = name;
      Secret = secret;
    }
  }

  public string Pattern => @"([^:\n]+):([^:\n]+)";

  public override string ToString() => StringValue;

  static (string name, string secret) Parse(string value) {
    var tokens = value.UnJoin(':').ToQueue();
    var name = tokens.TryDequeue();
    var secret = tokens.TryDequeue();
    return (name, secret);
  }

  public SecureString SecureString() {
    var ss = new SecureString();
    foreach (var c in Secret)
      ss.AppendChar(c);
    ss.MakeReadOnly();
    return ss;
  }
}