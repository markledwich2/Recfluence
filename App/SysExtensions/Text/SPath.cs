using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using SysExtensions.Collections;
using SysExtensions.Serialization;

namespace SysExtensions.Text; 

/// <summary>Represent a path (file systems, navigation) that easily converts to/from a string using the '/' separator</summary>
[TypeConverter(typeof(StringConverter<SPath>))]
public class SPath : IStringConvertable {
  protected const string UpToken = "..";

  public static SPath Emtpy = new();

  public SPath() : this(new string[] { }) { }

  public SPath(string path) => StringValue = path;

  public SPath(IEnumerable<string> tokens) => Tokens = tokens.ToArray();

  protected virtual char EscapeChar => '\\';
  protected virtual char Seperator  => '/';

  public IReadOnlyCollection<string> Tokens { get; private set; }

  public bool IsRoot     => Tokens.Count == 1 && IsAbsolute;
  public bool IsAbsolute { get; private set; }
  public bool IsRelative => !IsAbsolute;
  public bool IsFullPath => IsAbsolute && Tokens.All(t => t != UpToken);
  public bool IsEmpty    => Tokens.Count == 0;

  public SPath Parent => Tokens.Count <= 1
    ? new() {IsAbsolute = IsAbsolute}
    : new SPath(Tokens.Take(Tokens.Count - 1)) {IsAbsolute = IsAbsolute};

  public string Name => Tokens.LastOrDefault();

  /// <summary>Like a file extension, except anything after the first "." is considered part of the extension</summary>
  public string[] Extensions {
    get {
      var split = Name?.Split('.');
      if (split == null || split.Length == 1) return new string[] { };
      return split.Skip(1).ToArray();
    }
  }

  public string ExtensionsString => Extensions.Join(".");

  /// <summary>A name minus a file extension, except anything after the first "." is considered part of the extension</summary>
  public string NameSansExtension => Name?.Split('.').FirstOrDefault();

  public bool HasTailSeparator => Tokens.Count > 0 && Tokens.Last() == "";

  public string StringValue {
    get => (IsAbsolute ? Seperator.ToString() : "") + Tokens.Join(Seperator.ToString(), format: null, EscapeChar);

    // don't call this directly. Just for serialization and testing
    set {
      if (value == null) {
        Tokens = new string[] { };
        return;
      }

      if (value.StartsWith(Seperator.ToString())) {
        IsAbsolute = true;
        value = value.Substring(startIndex: 1, value.Length - 1);
      }

      if (value.HasValue())
        Tokens = value.UnJoin(Seperator, EscapeChar).ToList();
    }
  }

  public override string ToString() => StringValue;

  public SPath WithExtension(string extension) => Parent.Add($"{NameSansExtension}.{extension}");
  public SPath WithoutExtension() => Parent.Add(NameSansExtension);

  public SPath ToAbsolute() => IsAbsolute ? this : Absolute(Tokens);
  public static SPath Absolute(IEnumerable<string> tokens) => new(tokens) {IsAbsolute = true};
  public static SPath Absolute(params string[] tokens) => new(tokens) {IsAbsolute = true};

  public SPath Clone() => FromString(StringValue);

  public SPath Add(IEnumerable<string> path) => new(Tokens.Concat(path.NotNull())) {IsAbsolute = IsAbsolute};
  public SPath Add(SPath path) => Add(path.Tokens);
  public SPath Add(string token) => Add(token.InArray());
  public SPath Add(params string[] path) => Add((IEnumerable<string>) path);
  public SPath WithoutTailSeparator() => HasTailSeparator ? new(Tokens.Take(Tokens.Count - 1)) : this;
  public SPath WithTailSeparator() => HasTailSeparator ? this : new(Tokens.Concat(""));

  public override bool Equals(object obj) => obj?.ToString() == ToString();

  public static bool operator ==(SPath a, SPath b) {
    if (ReferenceEquals(a, objB: null) && ReferenceEquals(b, objB: null)) return true;
    if (ReferenceEquals(a, objB: null) || ReferenceEquals(b, objB: null)) return false;
    return a.Equals(b);
  }

  public static bool operator !=(SPath a, SPath b) => !(a == b);

  public static SPath FromString(string path) => new() {StringValue = path};

  /// <summary>If the path is relative. Then uses the full path context to convert to a full path</summary>
  public SPath FullPath(SPath fullPathContext) {
    if (!fullPathContext.IsFullPath) throw new InvalidOperationException("Expecting a full path context");
    if (IsAbsolute) throw new NotImplementedException();
    if (IsFullPath) return Clone();
    var path = fullPathContext.Clone();
    foreach (var t in Tokens)
      if (t == UpToken) {
        if (path.Parent == null) return null;
        path = path.Parent;
      }
      else {
        path = path.Add(t);
      }

    return path;
  }

  public override int GetHashCode() => ToString().GetHashCode();

  public static implicit operator string(SPath s) => s?.StringValue;
  public static implicit operator SPath(string s) => s == null ? null : new SPath(s);

  public static SPath Relative(params string[] tokens) => new(tokens) {IsAbsolute = false};

  /// <summary>Returns a relative path from the given path if one exists, otherwise returns a copy of this. Both paths must
  ///   be absolute</summary>
  /// <param name="from"></param>
  /// <returns></returns>
  public SPath RelativePath(SPath from) {
    var commonRoot = Tokens.CommonSequence(from.Tokens).AsCollection();
    if (commonRoot.IsEmpty())
      return Clone();

    var upTokens = from.Tokens.Skip(commonRoot.Count).Select(t => UpToken);
    var downTokens = Tokens.Skip(commonRoot.Count).Select(t => t);
    return new(upTokens.Concat(downTokens));
  }
}