using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Text;

namespace SysExtensions.Serialization {
  /// <summary>Provides lean access to serialiation funcitoanlity. Uses a good default's for serialization, but can be
  ///   overriden with any settings</summary>
  public static class JsonExtensions {
    public static JsonSerializer DefaultSerializer => JsonSerializer.Create(DefaultSettings());

    public static JsonLoadSettings DefaultLoadSettings => new();

    public static JsonSerializerSettings DefaultSettings(Formatting formatting = Formatting.Indented) {
      var settings = new JsonSerializerSettings {
        NullValueHandling = NullValueHandling.Ignore, Formatting = formatting, DefaultValueHandling = DefaultValueHandling.Ignore
      };
      settings.Converters.AddRange(new StringEnumConverter(new CamelCaseNamingStrategy(processDictionaryKeys: false, overrideSpecifiedNames: false),
        allowIntegerValues: false));
      settings.ContractResolver = new CoreSerializeContractResolver
        {NamingStrategy = new CamelCaseNamingStrategy(processDictionaryKeys: false, overrideSpecifiedNames: false)};
      return settings;
    }

    static readonly JsonSerializerSettings _defaultSettings = DefaultSettings();

    public static JsonSerializer Serializer(this JsonSerializerSettings settings) => JsonSerializer.Create(settings);

    public static T JsonClone<T>(this T source, JsonSerializerSettings settings = null) {
      settings ??= _defaultSettings;
      var serialized = JsonConvert.SerializeObject(source, settings);
      return JsonConvert.DeserializeObject<T>(serialized, settings);
    }

    public static T Deserialize<T>(this JsonSerializer serializer, TextReader reader) =>
      (T) serializer.Deserialize(reader, typeof(T));

    public static bool NullOrEmpty(this JToken token) =>
      token.Type == JTokenType.Null || token.Type == JTokenType.String && token.Value<string>().NullOrEmpty();

    public static T RemoveNullOrEmptyDescendants<T>(this T token) where T : JToken {
      switch (token.Type) {
        case JTokenType.Object: {
          var copy = new JObject();
          foreach (var prop in token.Children<JProperty>()) {
            var child = prop.Value;

            if (child.HasValues)
              child = RemoveNullOrEmptyDescendants(child);

            if (!child.NullOrEmpty())
              copy.Add(prop.Name, child);
          }
          return copy as T;
        }
        case JTokenType.Array: {
          foreach (var item in token.Children())
            item.RemoveNullOrEmptyDescendants();
          return token;
        }
      }
      return token;
    }

    public static JObject ToJObject(this object o, JsonSerializerSettings settings = null)
      => (JObject) SerializeToJToken(o, settings);

    /// <summary>Returns a new instance of T with targets values overriden by newValues non-null values Relies entirely on the
    ///   Newtonsoft.Json merging feature</summary>
    public static T JsonMerge<T>(this T target, T newValues, JsonMergeSettings mergeSettings = null, JsonSerializerSettings settings = null) {
      var aJ = target.ToJObject(settings);
      mergeSettings ??= new() {MergeNullValueHandling = MergeNullValueHandling.Ignore};
      aJ.Merge(newValues.ToJObject(settings), mergeSettings);
      return aJ.ToObject<T>(settings);
    }

    public static string ToJson<T>(this T o, JsonSerializerSettings settings = null) {
      settings ??= _defaultSettings;
      return JsonConvert.SerializeObject(o, settings);
    }

    public static Stream ToJsonStream<T>(this T o, JsonSerializerSettings settings = null, Encoding encoding = null) {
      settings ??= _defaultSettings;
      var ms = new MemoryStream();
      using (var sw = new StreamWriter(ms, leaveOpen: true, encoding: encoding ?? Encoding.UTF8))
      using (var jw = new JsonTextWriter(sw))
        settings.Serializer().Serialize(jw, o);
      ms.Seek(offset: 0, SeekOrigin.Begin);
      return ms;
    }

    public static void WriteJson(this FPath filePath, object o, JsonSerializerSettings settings = null)
      => o.ToJsonFile(filePath.FullPath, settings);

    public static void ToJsonFile(this object o, string filePath, JsonSerializerSettings settings = null) {
      settings ??= _defaultSettings;
      using (var fw = File.Open(filePath, FileMode.Create))
      using (var sw = new StreamWriter(fw))
      using (var jw = new JsonTextWriter(sw)) {
        var s = JsonSerializer.Create(settings);
        s.Serialize(jw, o);
      }
    }

    public static JObject ParseJObject(this string s, JsonLoadSettings loadSettings = null) => (JObject) s.ParseJToken(loadSettings);
    public static JToken ParseJToken(this string s, JsonLoadSettings loadSettings = null) => JToken.Parse(s, loadSettings);

    public static JToken SerializeToJToken(this object o, JsonSerializerSettings settings = null) {
      settings ??= _defaultSettings;
      return JToken.FromObject(o, JsonSerializer.Create(settings));
    }

    public static T ToObject<T>(this JToken j, JsonSerializerSettings settings = null) {
      settings ??= _defaultSettings;
      return j.ToObject<T>(JsonSerializer.Create(settings));
    }

    public static T ToObject<T>(this FPath file, JsonSerializerSettings settings = null, JsonLoadSettings loadSettings = null) {
      using (var tr = file.OpenText()) return tr.ToObject<T>(settings, loadSettings);
    }

    public static T ToObject<T>(this Stream reader, JsonSerializerSettings settings = null, JsonLoadSettings loadSettings = null) {
      using var tr = new StreamReader(reader, Encoding.UTF8, leaveOpen: true);
      return tr.ToObject<T>(settings, loadSettings);
    }

    public static T ToObject<T>(this TextReader reader, JsonSerializerSettings settings = null, JsonLoadSettings loadSettings = null) {
      settings ??= _defaultSettings;
      loadSettings ??= DefaultLoadSettings;
      var jsonReader = new JsonTextReader(reader);
      var serilaizer = JsonSerializer.Create(settings);
      return serilaizer.Deserialize<T>(JToken.Load(jsonReader, loadSettings).CreateReader());
      // have to go in a roundabout way to be able to give the loader settings. This is the best way to ignore commends when it comes from a file
    }

    public static T ToObject<T>(this string json, JsonSerializerSettings settings = null) {
      settings ??= _defaultSettings;
      return JsonConvert.DeserializeObject<T>(json, settings);
    }

    public static JObject ToCamelCase(this JObject original) {
      var newObj = new JObject();
      foreach (var property in original.Properties()) {
        var newPropertyName = property.Name.ToCamelCase();
        newObj[newPropertyName] = property.Value.ToCamelCaseJToken();
      }
      return newObj;
    }

    public static JToken ToCamelCaseJToken(this JToken original) =>
      original.Type switch {
        JTokenType.Object => ((JObject) original).ToCamelCase(),
        JTokenType.Array => new JArray(((JArray) original).Select(x => x.ToCamelCaseJToken())),
        _ => original.DeepClone()
      };

    /// <summary>Shorthand for t.Value<string>()</summary>
    public static string Str(this JToken t, string path = null) => (path == null ? t : t.Token(path))?.Value<string>();

    public static JToken Token(this JToken t, string path) => t.SelectToken(path);
    public static IEnumerable<JToken> Tokens(this JToken t, string path) => t.SelectTokens(path);
  }
}