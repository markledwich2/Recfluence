using System;
using System.Collections;
using System.ComponentModel;
using System.Reflection;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using SysExtensions.Reflection;

namespace SysExtensions.Serialization {
  /// <summary>Camel case properties. Also when using OptOut, then only public properties that are writable are serialized by
  ///   default</summary>
  public class CoreSerializeContractResolver : CamelCasePropertyNamesContractResolver {
    protected override JsonProperty CreateProperty(MemberInfo member, MemberSerialization memberSerialization) {
      var prop = base.CreateProperty(member, memberSerialization);
      if (memberSerialization != MemberSerialization.OptOut) return prop;

      // classes with no default constructors should serialize as per normal
      var emptyConstructor = member.DeclaringType?.GetConstructor(Type.EmptyTypes);
      if (emptyConstructor != null) return prop;

      // by default only writable properties should be serialized
      if (!prop.Writable && !prop.PropertyType.IsCollection()
                         && member.GetCustomAttribute<JsonPropertyAttribute>(true) == null)
        return null;
      return prop;
    }

    /// <summary>Determines which contract type is created for the given type.</summary>
    /// <param name="objectType">Type of the object.</param>
    /// <returns>A <see cref="JsonContract" /> for the given type.</returns>
    protected override JsonContract CreateContract(Type objectType) {
      var contract = base.CreateContract(objectType);

      // by default a type that can convert to string and that is also an enum will have an array contract, but serialize to a string!. fix  this
      if (contract is JsonArrayContract && typeof(IEnumerable).IsAssignableFrom(objectType) &&
          CanNonSystemTypeDescriptorConvertString(objectType, out var converter))
        contract = CreateStringContract(objectType);
      return contract;
    }

    public static bool CanNonSystemTypeDescriptorConvertString(Type type, out TypeConverter typeConverter) {
      typeConverter = TypeDescriptor.GetConverter(type);

      // use the objectType's TypeConverter if it has one and can convert to a string
      var converterType = typeConverter.GetType();
      if (!converterType.FullName.StartsWith("System.ComponentModel") && converterType != typeof(TypeConverter)) {
        var canConvert = typeConverter.CanConvertTo(typeof(string));
        return canConvert;
      }
      return false;
    }
  }
}