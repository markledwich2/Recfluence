using System.Linq.Expressions;
using System.Reflection;

namespace SysExtensions.Reflection {
  public static class ExpressionExtensions {
    public static object GetValue(this Expression expression) => Expression.Lambda(expression).Compile().DynamicInvoke();
  }
}