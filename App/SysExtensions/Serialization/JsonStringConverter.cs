using System.ComponentModel;
using System.Globalization;
using Newtonsoft.Json;

namespace SysExtensions.Serialization;

/// <summary>Represents a type that is meant to be converted to and from strings for serialization</summary>
public interface IStringConvertable {
  string StringValue { get; set; }
  string ToString();
}

public interface IStringConvertableWithPattern : IStringConvertable {
  string Pattern { get; }
}

public class StringConverter<T> : TypeConverter where T : IStringConvertable, new() {
  public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
    => sourceType == typeof(string) || base.CanConvertFrom(context, sourceType);

  public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value) {
    if (value is string)
      return new T { StringValue = value.ToString() };
    return base.ConvertFrom(context, culture, value);
  }
}

/// <summary>like string convertible, but the resulting type can be dynamic</summary>
public interface IStringParsable<out T> {
  T Instance(string value);
  string ToString();
}

public class StringParser<T> : TypeConverter where T : IStringParsable<T>, new() {
  public override bool CanConvertFrom(ITypeDescriptorContext context, Type sourceType)
    => sourceType == typeof(string) || base.CanConvertFrom(context, sourceType);

  public override object ConvertFrom(ITypeDescriptorContext context, CultureInfo culture, object value) {
    if (value is string s)
      return new T().Instance(s);
    return base.ConvertFrom(context, culture, value);
  }
}

/// <summary>Will convert any class to a string (using ToString()). And from a string  with IConvertable, or with the
///   default constructor + IStringConvertable. Use on properties. if you have a class you should decorate with
///   [TypeConverter(typeof(StringConverter<T>))] instead</summary>
public class JsonStringConverter : JsonConverter {
  public override bool CanConvert(Type objectType)
    => IsTypeConvertable(objectType) || IsStringConvertable(objectType);

  public static bool IsStringConvertable(Type objectType) =>
    typeof(IStringConvertable).IsAssignableFrom(objectType);

  public static bool IsTypeConvertable(Type objectType) =>
    TypeDescriptor.GetConverter(objectType)?.CanConvertTo(typeof(string)) ?? false;

  public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer) =>
    reader.Value == null ? null : ConvertFromString((string)reader.Value, objectType);

  public static T ConvertFromString<T>(string value) => (T)ConvertFromString(value, typeof(T));

  public static object ConvertFromString(string value, Type objectType) {
    if (IsStringConvertable(objectType)) {
      var o = (IStringConvertable)Activator.CreateInstance(objectType);
      o.StringValue = value;
      return o;
    }

    var converter = TypeDescriptor.GetConverter(objectType);
    if (converter != null && converter.CanConvertFrom(typeof(string)))
      return converter.ConvertFromString(value);

    throw new InvalidOperationException($"Can't convert from string to type {objectType.Name}");
  }

  public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer) => writer.WriteValue(value?.ToString());
}