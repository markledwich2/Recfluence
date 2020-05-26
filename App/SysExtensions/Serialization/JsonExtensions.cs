﻿using System;
using System.ComponentModel;
using System.Globalization;
using System.IO;
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

    public static JsonLoadSettings DefaultLoadSettings => new JsonLoadSettings();

    public static JsonSerializerSettings DefaultSettings(Formatting formatting = Formatting.Indented) {
      var settings = new JsonSerializerSettings {
        NullValueHandling = NullValueHandling.Ignore, Formatting = formatting, DefaultValueHandling = DefaultValueHandling.Ignore
      };
      settings.Converters.AddRange(new StringEnumConverter(new CamelCaseNamingStrategy(false, false), false));
      settings.ContractResolver = new CoreSerializeContractResolver {NamingStrategy = new CamelCaseNamingStrategy(false, false)};
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

    /// <summary>Returns a new instance of T with targets values overriden by newValues non-null values
    // Relies entirely on the Newtonsoft.Json merging feature</summary>
    public static T JsonMerge<T>(this T target, T newValues, JsonSerializerSettings settings = null) {
      var aJ = target.ToJObject(settings);
      aJ.Merge(newValues.ToJObject(settings), new JsonMergeSettings {MergeNullValueHandling = MergeNullValueHandling.Ignore});
      return aJ.ToObject<T>(settings);
    }

    public static string ToJson<T>(this T o, JsonSerializerSettings settings = null) {
      settings ??= _defaultSettings;
      return JsonConvert.SerializeObject(o, settings);
    }

    public static Stream ToJsonStream<T>(this T o, JsonSerializerSettings settings = null) {
      settings ??= _defaultSettings;
      var ms = new MemoryStream();
      using (var sw = new StreamWriter(ms, leaveOpen: true))
      using (var jw = new JsonTextWriter(sw))
        settings.Serializer().Serialize(jw, o);
      ms.Seek(0, SeekOrigin.Begin);
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

    public static T TryToObject<T>(this string json, JsonSerializerSettings settings = null) {
      try {
        return json.ToObject<T>(settings);
      }
      catch { }

      return default;
    }

    /// <summary>When an object is serialized to JObject's, some serialization formatting hasn't been applied yet (e.g. date
    ///   time formatting). You must also use settings when outputting a string from the JToken</summary>
    /// <param name="token"></param>
    /// <param name="formatting"></param>
    /// <param name="settings"></param>
    /// <returns></returns>
    public static string ToString(this JToken token, Formatting formatting = Formatting.Indented, JsonSerializerSettings settings = null) {
      using (var sw = new StringWriter(CultureInfo.InvariantCulture)) {
        using (var jsonWriter = new JsonTextWriter(sw)) {
          jsonWriter.Formatting = formatting;
          jsonWriter.Culture = CultureInfo.InvariantCulture;
          if (settings != null) {
            jsonWriter.DateFormatHandling = settings.DateFormatHandling;
            jsonWriter.DateFormatString = settings.DateFormatString;
            jsonWriter.DateTimeZoneHandling = settings.DateTimeZoneHandling;
            jsonWriter.FloatFormatHandling = settings.FloatFormatHandling;
            jsonWriter.StringEscapeHandling = settings.StringEscapeHandling;
          }
          token.WriteTo(jsonWriter);
        }
        return sw.ToString();
      }
    }

    public static T PropertyValue<T>(this JObject jObject, string name, JsonSerializerSettings settings = null) {
      var prop = jObject.Property(name);
      if (prop == null) return default;
      return prop.PropertyValue<T>(settings);
    }

    /// <summary>Returns the .NET property value if it exists, null otherwise. Automatically converts types and deserializes
    ///   string if required</summary>
    public static T PropertyValue<T>(this JProperty jProp, JsonSerializerSettings settings = null) {
      var value = (jProp?.Value as JValue)?.Value;
      if (value == null) return default;
      if (value is T) return (T) value;

      var converter = TypeDescriptor.GetConverter(typeof(T));
      if (converter.CanConvertFrom(value.GetType()))
        return (T) converter.ConvertFrom(value);

      // if the value is a string that can't be converted to the given type. Deserialize it
      if (value is string s)
        return s.ToObject<T>(settings);

      throw new InvalidOperationException($"Unable to convert value of {value.GetType()} to {typeof(T)}");
    }
  }
}