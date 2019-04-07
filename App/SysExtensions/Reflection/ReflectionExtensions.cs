using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace SysExtensions.Reflection {
  public enum CopyPropertiesBehaviour {
    CopyAll,
    IfNotNull
  }

  public static class ReflectionExtensions {
      /// <summary>
      ///   Coppies properties and all items in ICollection<> properties
      /// </summary>
      public static void CopyPropertiesFrom<T>(this T to, object from, CopyPropertiesBehaviour behaviour = CopyPropertiesBehaviour.CopyAll)
      where T : class {
      foreach (var toProp in to.GetType().GetProperties()) {
        var fromProp = from.GetType().GetProperty(toProp.Name, toProp.PropertyType);
        if (fromProp == null) continue;

        // set any setter properties
        if (toProp.GetSetMethod() != null && toProp.GetIndexParameters().Length == 0 && toProp.GetIndexParameters().Length == 0)
          switch (behaviour) {
            case CopyPropertiesBehaviour.CopyAll:
              SetValue(to, from, toProp);
              break;
            case CopyPropertiesBehaviour.IfNotNull:
              var existingValue = toProp.GetValue(to);
              if (existingValue == toProp.PropertyType.DefaultForType())
                SetValue(to, from, toProp);
              break;
            default:
              throw new ArgumentOutOfRangeException(nameof(behaviour), behaviour, null);
          }

        if (toProp.PropertyType.GetTypeInfo().IsGenericType && typeof(ICollection<>).IsAssignableFrom(toProp.PropertyType
                                                              .GetGenericTypeDefinition())
                                                            && typeof(IEnumerable).IsAssignableFrom(fromProp.PropertyType)) {
          var toCollection = toProp.GetValue(to, null);
          var addMethod = toCollection.GetType().GetMethod("Add");
          var fromEnumerable = (IEnumerable) fromProp.GetValue(from, null);
          foreach (var item in fromEnumerable)
            addMethod.Invoke(toCollection, new[] {item});
        }
      }
    }

    public static T ShallowClone<T>(this T from) where T : class, new() {
      var to = new T();
      to.CopyPropertiesFrom(from);
      return to;
    }

    public static object DefaultForType(this Type type) => type.GetTypeInfo().IsValueType ? Activator.CreateInstance(type) : null;

    public static IEnumerable<Type> ImplementingTypes<T>(this Assembly assembly) where T : class =>
      assembly.ExportedTypes.Where(t => IsCreatable(t) && typeof(T).IsAssignableFrom(t));

    public static bool ImplementsGenericDefinition(this Type type, Type genericInterfaceDefinition) {
      if (!genericInterfaceDefinition.GetTypeInfo().IsInterface || !genericInterfaceDefinition.GetTypeInfo().IsGenericTypeDefinition)
        throw new ArgumentNullException($"'{genericInterfaceDefinition}' is not a generic interface definition.");

      if (type.GetTypeInfo().IsInterface)
        if (type.GetTypeInfo().IsGenericType && genericInterfaceDefinition == type.GetGenericTypeDefinition())
          return true;

      foreach (var i in type.GetInterfaces())
        if (i.GetTypeInfo().IsGenericType && genericInterfaceDefinition == i.GetGenericTypeDefinition())
          return true;

      return false;
    }

    /// <summary>
    ///   Assumed to be a collection if it is enumerable and has an add method
    /// </summary>
    /// <param name="type"></param>
    /// <returns></returns>
    public static bool IsCollection(this Type type) => type.ImplementsGenericDefinition(typeof(ICollection<>));

    public static bool IsEnumerable(this Type type) => typeof(IEnumerable).IsAssignableFrom(type);

    public static bool IsNullable(this Type t) {
      if (t.GetTypeInfo().IsValueType)
        return t.GetTypeInfo().IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>);
      return true;
    }

    static void SetValue<T>(T to, object from, PropertyInfo toProp) where T : class =>
      toProp.SetValue(to, toProp.GetValue(from, null), null);

    public static object GetPropValue(this object o, string propName) => o.GetType().GetProperty(propName).GetValue(o);

    public static T GetPropValue<T>(this object o, string propName) => (T) o.GetPropValue(propName);

    public static void SetPropValue(this object o, string propName, object value) => o.GetType().GetProperty(propName).SetValue(o, value);

    static bool IsCreatable(Type type) {
      var info = type.GetTypeInfo();
      return
        info.IsClass && info.IsPublic && !info.IsAbstract && !info.IsEnum && !info.IsInterface &&
        !info.IsGenericTypeDefinition && !info.IsNested;
    }
  }
}