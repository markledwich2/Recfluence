using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using SysExtensions.Text;

namespace SysExtensions.Reflection; 

public enum CopyPropertiesBehaviour {
  CopyAll,
  SkipDefault
}

public static class ReflectionExtensions {
  /// <summary>Copies non-default properties and all items in ICollection<> properties</summary>
  public static void CopyPropertiesFrom<T>(this T to, object from)
    where T : class {
    foreach (var toProp in to.GetType().GetProperties()) {
      var fromProp = from.GetType().GetProperty(toProp.Name, toProp.PropertyType);
      if (fromProp == null) continue;
      var fromValue = from.GetPropValue(fromProp.Name);

      // don't copy if values are default for their type
      if (fromValue.EqualsSafe(fromProp.PropertyType.DefaultForType()))
        continue;

      // set any setter properties
      if (toProp.GetSetMethod() != null && toProp.GetIndexParameters().Length == 0 && toProp.GetIndexParameters().Length == 0) {
        toProp.SetValue(to, fromValue);
        continue;
      }

      if (!toProp.PropertyType.GetTypeInfo().IsGenericType || !typeof(ICollection<>).IsAssignableFrom(toProp.PropertyType
            .GetGenericTypeDefinition()) || !typeof(IEnumerable).IsAssignableFrom(fromProp.PropertyType)) continue;

      var toCollection = toProp.GetValue(to, index: null) ?? throw new InvalidOperationException("collection not set-able or ad-able");
      var addMethod = toCollection.GetType().GetMethod("Add") ?? throw new InvalidOperationException("collection not set-able or ad-able");
      foreach (var item in (IEnumerable) fromProp.GetValue(from, index: null) ?? new object[] { })
        addMethod.Invoke(toCollection, new[] {item});
    }
  }

  public static T ShallowClone<T>(this T from) where T : class, new() {
    var to = new T();
    to.CopyPropertiesFrom(from);
    return to;
  }

  /// <summary>Makes a shallow clone of the object and sets non-default properties from with (slow)</summary>
  public static T ShallowWith<T>(this T from, T with) where T : class, new() {
    var clone = from.ShallowClone();
    clone.CopyPropertiesFrom(with);
    return clone;
  }

  public static bool NullOrDefault<T>(this T value) => EqualityComparer<T>.Default.Equals(value, y: default);

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

  /// <summary>Assumed to be a collection if it is enumerable and has an add method</summary>
  /// <param name="type"></param>
  /// <returns></returns>
  public static bool IsCollection(this Type type) => type.ImplementsGenericDefinition(typeof(ICollection<>));

  public static bool IsEnumerable(this Type type) => typeof(IEnumerable).IsAssignableFrom(type);

  public static bool IsNullable(this Type t) {
    if (t.GetTypeInfo().IsValueType)
      return t.GetTypeInfo().IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>);
    return true;
  }

  /// <summary>A null safe equals</summary>
  public static bool EqualsSafe(this object a, object b) => a?.Equals(b) ?? b == null;

  public static object GetPropValue(this object o, string propName) => o.GetType().GetProperty(propName).GetValue(o);

  public static T GetPropValue<T>(this object o, string propName) => (T) o.GetPropValue(propName);

  public static void SetPropValue(this object o, string propName, object value) => o.GetType().GetProperty(propName).SetValue(o, value);

  static bool IsCreatable(Type type) {
    var info = type.GetTypeInfo();
    return
      info.IsClass && info.IsPublic && !info.IsAbstract && !info.IsEnum && !info.IsInterface &&
      !info.IsGenericTypeDefinition && !info.IsNested;
  }

  public static MethodInfo GetMethodInfo(Action action) => action.Method;

  public static MethodInfo GetMethodInfo<T>(Action<T> action) => action.Method;

  //public static MethodInfo GetMethodInfo<T,U>(Action<T,U> action) => action.Method;
  public static MethodInfo GetMethodInfo<TResult>(Func<TResult> fun) => fun.Method;
  public static MethodInfo GetMethodInfo<T, TResult>(Func<T, TResult> fun) => fun.Method;
  public static MethodInfo GetMethodInfo<T, U, TResult>(Func<T, U, TResult> fun) => fun.Method;

  public static async Task<TOut> CallStaticGenericTask<TOut>(this MethodInfo methodInfo, Type[] generics, params object[] args) {
    var loadInStateMethod = methodInfo?.MakeGenericMethod(generics)
      ?? throw new InvalidOperationException($"{nameof(methodInfo)}<{generics.Join(", ", g => g.Name)}> method not found ");
    dynamic task = loadInStateMethod.Invoke(obj: null, args);
    return (TOut) await task;
  }

  public static object MergeDynamics(object a, object b) {
    var result = new ExpandoObject();
    var d = (IDictionary<string, object>) result;
    foreach (var pair in GetKeyValueMap(a).Concat(GetKeyValueMap(b)))
      d[pair.Key] = pair.Value;
    return result;
  }

  static IDictionary<string, object> GetKeyValueMap(object values) =>
    values switch {
      null => new Dictionary<string, object>(),
      IDictionary<string, object> d => d,
      _ => TypeDescriptor.GetProperties(values).Cast<PropertyDescriptor>().ToDictionary(p => p.Name, p => p.GetValue(values))
    };
}