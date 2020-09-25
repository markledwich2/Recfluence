using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using SysExtensions.Collections;
using SysExtensions.Serialization;

namespace SysExtensions.Text {
  /// <summary>Represent a path (file systems, navigation) that easily converts to/from a string using the '/' separator</summary>
  [TypeConverter(typeof(StringConverter<StringPath>))]
  public class StringPath : IStringConvertable {
    protected const string UpToken = "..";

    public static StringPath Emtpy = new StringPath();

    public StringPath() : this(new string[] { }) { }

    public StringPath(string path) => StringValue = path;

    public StringPath(IEnumerable<string> tokens) => Tokens = tokens.ToArray();

    protected virtual char EscapeChar => '\\';
    protected virtual char Seperator  => '/';

    public IReadOnlyCollection<string> Tokens { get; private set; }

    public bool IsRoot     => Tokens.Count == 1 && IsAbsolute;
    public bool IsAbsolute { get; private set; }
    public bool IsRelative => !IsAbsolute;
    public bool IsFullPath => IsAbsolute && Tokens.All(t => t != UpToken);
    public bool IsEmpty    => Tokens.Count == 0;

    public StringPath Parent => Tokens.Count <= 1
      ? new StringPath {IsAbsolute = IsAbsolute}
      : new StringPath(Tokens.Take(Tokens.Count - 1)) {IsAbsolute = IsAbsolute};

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
      get => (IsAbsolute ? Seperator.ToString() : "") + Tokens.Join(Seperator.ToString(), null, EscapeChar);

      // don't call this directly. Just for serialization and testing
      set {
        if (value == null) {
          Tokens = new string[] { };
          return;
        }

        if (value.StartsWith(Seperator.ToString())) {
          IsAbsolute = true;
          value = value.Substring(1, value.Length - 1);
        }

        if (value.HasValue())
          Tokens = value.UnJoin(Seperator, EscapeChar).ToList();
      }
    }

    public override string ToString() => StringValue;

    public StringPath WithExtension(string extension) => Parent.Add(NameSansExtension + extension);
    public StringPath WithoutExtension() => Parent.Add(NameSansExtension);

    public StringPath ToAbsolute() => IsAbsolute ? this : Absolute(Tokens);
    public static StringPath Absolute(IEnumerable<string> tokens) => new StringPath(tokens) {IsAbsolute = true};
    public static StringPath Absolute(params string[] tokens) => new StringPath(tokens) {IsAbsolute = true};

    public StringPath Clone() => FromString(StringValue);

    public StringPath Add(IEnumerable<string> path) => new StringPath(Tokens.Concat(path.NotNull())) {IsAbsolute = IsAbsolute};
    public StringPath Add(StringPath path) => Add(path.Tokens);
    public StringPath Add(string token) => Add(token.InArray());
    public StringPath Add(params string[] path) => Add((IEnumerable<string>) path);
    public StringPath WithoutTailSeparator() => HasTailSeparator ? new StringPath(Tokens.Take(Tokens.Count - 1)) : this;
    public StringPath WithTailSeparator() => HasTailSeparator ? this : new StringPath(Tokens.Concat(""));

    public override bool Equals(object obj) => obj?.ToString() == ToString();

    public static bool operator ==(StringPath a, StringPath b) {
      if (ReferenceEquals(a, null) && ReferenceEquals(b, null)) return true;
      if (ReferenceEquals(a, null) || ReferenceEquals(b, null)) return false;
      return a.Equals(b);
    }

    public static bool operator !=(StringPath a, StringPath b) => !(a == b);

    public static StringPath FromString(string path) => new StringPath {StringValue = path};

    /// <summary>If the path is relative. Then uses the full path context to convert to a full path</summary>
    public StringPath FullPath(StringPath fullPathContext) {
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

    public static implicit operator string(StringPath s) => s?.StringValue;
    public static implicit operator StringPath(string s) => s == null ? null : new StringPath(s);

    public static StringPath Relative(params string[] tokens) => new StringPath(tokens) {IsAbsolute = false};

    /// <summary>Returns a relative path from the given path if one exists, otherwise returns a copy of this. Both paths must
    ///   be absolute</summary>
    /// <param name="from"></param>
    /// <returns></returns>
    public StringPath RelativePath(StringPath from) {
      var commonRoot = Tokens.CommonSequence(from.Tokens).AsCollection();
      if (commonRoot.IsEmpty())
        return Clone();

      var upTokens = from.Tokens.Skip(commonRoot.Count).Select(t => UpToken);
      var downTokens = Tokens.Skip(commonRoot.Count).Select(t => t);
      return new StringPath(upTokens.Concat(downTokens));
    }
  }
}