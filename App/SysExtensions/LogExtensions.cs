namespace SysExtensions;

public static class LogExtensions {
  public static ILogger Scope(this ILogger log, string scope) => log.ForContext("Scope", scope);
}