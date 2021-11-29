using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using SysExtensions.Reflection;

namespace SysExtensions.Configuration; 

public class SkipRecursiveValidation : Attribute { }

/// <summary>From
///   https://github.com/reustmd/DataAnnotationsValidatorRecursive/blob/master/DataAnnotationsValidator/DataAnnotationsValidator/DataAnnotationsValidator.cs</summary>
public class DataAnnotationsValidator {
  public bool TryValidateObject(object obj, ICollection<ValidationResult> results, IDictionary<object, object> validationContextItems = null) =>
    Validator.TryValidateObject(obj, new ValidationContext(obj, serviceProvider: null, validationContextItems), results, validateAllProperties: true);

  public (bool valid, ValidationResult[] results) TryValidateObjectRecursive<T>(T obj, IDictionary<object, object> validationContextItems = null) {
    var results = new List<ValidationResult>();
    var valid = TryValidateObjectRecursive(obj, results, new HashSet<object>(), validationContextItems);
    return (valid, results.ToArray());
  }

  bool TryValidateObjectRecursive<T>(T obj, List<ValidationResult> results, ISet<object> validatedObjects,
    IDictionary<object, object> validationContextItems = null) {
    //short-circuit to avoid infinit loops on cyclical object graphs
    if (validatedObjects.Contains(obj)) return true;

    validatedObjects.Add(obj);
    var result = TryValidateObject(obj, results, validationContextItems);

    var properties = obj.GetType().GetProperties().Where(prop => prop.CanRead
      && !prop.GetCustomAttributes(typeof(SkipRecursiveValidation), inherit: false).Any()
      && prop.GetIndexParameters().Length == 0).ToList();

    foreach (var property in properties) {
      if (property.PropertyType == typeof(string) || property.PropertyType.IsValueType) continue;

      var value = obj.GetPropValue(property.Name);

      if (value == null) continue;

      var asEnumerable = value as IEnumerable;
      if (asEnumerable != null) {
        foreach (var enumObj in asEnumerable)
          if (enumObj != null) {
            var nestedResults = new List<ValidationResult>();
            if (!TryValidateObjectRecursive(enumObj, nestedResults, validatedObjects, validationContextItems)) {
              result = false;
              foreach (var validationResult in nestedResults) {
                var property1 = property;
                results.Add(new ValidationResult(validationResult.ErrorMessage, validationResult.MemberNames.Select(x => property1.Name + '.' + x)));
              }
            }
            ;
          }
      }
      else {
        var nestedResults = new List<ValidationResult>();
        if (!TryValidateObjectRecursive(value, nestedResults, validatedObjects, validationContextItems)) {
          result = false;
          foreach (var validationResult in nestedResults) {
            var property1 = property;
            results.Add(new ValidationResult(validationResult.ErrorMessage, validationResult.MemberNames.Select(x => property1.Name + '.' + x)));
          }
        }
        ;
      }
    }

    return result;
  }
}