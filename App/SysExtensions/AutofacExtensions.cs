namespace SysExtensions; 

/// <summary>Use for generic type interence</summary>
/// <typeparam name="T">the type of the parameter</typeparam>
public static class Typ {
  public static Of<U> Of<U>() => new Of<U>();

  public static string Hell() => "string";
}

public class Of<T> { }