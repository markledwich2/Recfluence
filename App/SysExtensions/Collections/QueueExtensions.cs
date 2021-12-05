namespace SysExtensions.Collections;

public static class QueueExtensions {
  public static IEnumerable<T> Dequeue<T>(this Queue<T> queue, int number) {
    for (var i = 0; i < number; i++) {
      var t = queue.TryDequeue();
      if (t == null) break;
      yield return t;
    }
  }

  public static void Enqueue<T>(this Queue<T> queue, IEnumerable<T> items) {
    foreach (var item in items)
      queue.Enqueue(item);
  }

  public static Queue<T> ToQueue<T>(this IEnumerable<T> items) => new Queue<T>(items);

  public static Stack<T> ToStack<T>(this IEnumerable<T> items) => new Stack<T>(items);

  public static T TryPop<T>(this Stack<T> stack) => stack.Count > 0 ? stack.Pop() : default;

  public static T TryDequeue<T>(this Queue<T> queue) => queue.Count > 0 ? queue.Dequeue() : default;
}