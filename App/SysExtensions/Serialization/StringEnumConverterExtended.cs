using System;
using System.Reflection;
using Humanizer;
using Newtonsoft.Json;
using SysExtensions.Reflection;

namespace SysExtensions.Serialization; 

/// <summary>An alternative string enum converter that ignores explicitly named enums and allows different types of casing</summary>
public class StringEnumConverterExtended : JsonConverter {
  public EnumCasing? Casing { get; set; }

  public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer) {
    if (reader.TokenType == JsonToken.Null) {
      if (!objectType.IsNullable())
        throw new JsonSerializationException($"Cannot convert null value to {objectType}");
      return null;
    }

    var isNullable = objectType.IsNullable();
    var t = isNullable ? Nullable.GetUnderlyingType(objectType) : objectType;

    try {
      switch (reader.TokenType) {
        case JsonToken.String:
          var enumText = reader.Value.ToString();
          return enumText.ParseEnum<Enum>(defaultEnumString: DefaultEnumString, t: t);
        case JsonToken.Integer:
          throw new JsonSerializationException($"Integer value {reader.Value} is not allowed.");
      }
    }
    catch (Exception) {
      throw new JsonSerializationException($"Error converting value {reader.Value} to type '{objectType}'.");
    }

    // we don't actually expect to get here.
    throw new JsonSerializationException($"Unexpected token {reader.TokenType} when parsing enum.");
  }

  public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer) {
    if (value == null) {
      writer.WriteNull();
      return;
    }

    var e = (Enum) value;
    var name = e.EnumExplicitName() ?? DefaultEnumString(e);
    writer.WriteValue(name);
  }

  string DefaultEnumString(Enum e) {
    var name = e.ToString();
    if (!Casing.HasValue) return name;
    switch (Casing.Value) {
      case EnumCasing.UpperSnake:
        return name.Underscore().ToUpper();
      case EnumCasing.Snake:
        return name.Underscore().ToLower();
      case EnumCasing.Pascal:
        return name.Pascalize();
      case EnumCasing.Camel:
        return name.Camelize();
    }

    return name;
  }

  public override bool CanConvert(Type objectType) =>
    objectType.GetTypeInfo().IsEnum ||
    objectType.IsNullable() && Nullable.GetUnderlyingType(objectType)?.GetTypeInfo().IsEnum == true;
}

public enum EnumCasing {
  UpperSnake,
  Snake,
  Pascal,
  Camel
}