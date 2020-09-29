using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Humanizer;
using Nito.AsyncEx;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Text;
using static Mutuo.Etl.Pipe.GraphTaskStatus;

namespace Mutuo.Etl.Pipe {
  public class GraphTask {
    public GraphTask(string name, string[] dependsOn, Func<CancellationToken, Task> run) {
      Run = run;
      Name = name;
      DependsOn = dependsOn;
    }

    public Func<CancellationToken, Task> Run       { get; set; }
    public string                        Name      { get; set; }
    public string[]                      DependsOn { get; set; }
    public GraphTaskStatus               Status    { get; set; }
  }

  public enum GraphTaskStatus {
    Available,
    Ignored,
    Queued,
    Running,
    Success,
    Error,
    Cancelled
  }

  public class GraphTaskResult {
    public string          Name        { get; set; }
    public bool            Success     => FinalStatus == GraphTaskStatus.Success;
    public bool            Error       => FinalStatus == GraphTaskStatus.Error;
    public GraphTaskStatus FinalStatus { get; set; }
    public Exception       Exception   { get; set; }
    public TimeSpan        Duration    { get; set; } = TimeSpan.Zero;

    public override string ToString() => $"{Name} ({FinalStatus}) in {Duration.HumanizeShort()}"
                                         + (Exception != null ? $". Error: {Exception.Message}" : "");
  }

  public static class JobProcessStatusExtensions {
    public static bool IsComplete(this GraphTaskStatus status) => !status.IsIncomplete();
    public static bool IsIncomplete(this GraphTaskStatus status) => !status.In(Error, Success, Cancelled, Ignored);
  }

  public class TaskGraph {
    readonly DependencyGraph<GraphTask> _graph;

    public TaskGraph(IEnumerable<GraphTask> jobs) => _graph = CreateGraph(jobs.ToArray());

    public static TaskGraph FromMethods(params Expression<Func<CancellationToken, Task>>[] methods) =>
      new TaskGraph(methods.Select(t => GraphTask(t)).ToArray());

    public static GraphTask GraphTask(Expression<Func<CancellationToken, Task>> expression, params string[] dependsOn) {
      var runTask = expression.Compile();
      var m = expression.Body as MethodCallExpression ?? throw new InvalidOperationException("expected an expression that calls a method");
      var attribute = m.Method.GetCustomAttribute<GraphTaskAttribute>();
      if (attribute != null)
        dependsOn = dependsOn.Concat(attribute.Deps).ToArray();
      return new GraphTask(attribute?.Name ?? m.Method.Name, dependsOn, runTask);
    }

    public bool AllComplete => _graph.Nodes.All(v => v.Status.IsComplete());

    public long Count => _graph.Nodes.Count;

    public IEnumerable<GraphTask> All => _graph.Nodes;

    public IEnumerable<GraphTask> Running => _graph.Nodes.Where(j => j.Status == GraphTaskStatus.Running);

    public IEnumerable<GraphTask> Available() => All.Where(t => t.Status == GraphTaskStatus.Available);

    public IEnumerable<GraphTask> AvailableToRun() =>
      _graph.Nodes
        .Where(j => !HasIncompleteDependencies(_graph, j) && j.Status == GraphTaskStatus.Available);

    public IEnumerable<GraphTask> Dependants(GraphTask node) => _graph.Dependants(node);

    public IEnumerable<GraphTask> Dependencies(GraphTask node) => _graph.Dependencies(node);

    public IEnumerable<GraphTask> DependenciesDeep(GraphTask node) => _graph.DependenciesDeep(node);

    DependencyGraph<GraphTask> CreateGraph(GraphTask[] jobs) =>
      new DependencyGraph<GraphTask>(jobs, n => n.DependsOn, j => j.Name);

    static bool HasIncompleteDependencies(DependencyGraph<GraphTask> graph, GraphTask job) =>
      graph.Dependencies(job).Any(j => j.Status.IsIncomplete());

    public GraphTask this[string name] => _graph[name];
  }

  [AttributeUsage(AttributeTargets.Method, Inherited = false)]
  public sealed class GraphTaskAttribute : Attribute {
    public GraphTaskAttribute(params string[] deps) => Deps = deps;
    public string[] Deps { get; }
    public string   Name { get; set; }
  }

  public static class TaskGraphEx {

    public static void IgnoreNotIncluded(this TaskGraph graph, IReadOnlyCollection<string> included) {
      if (included.None()) return;
      foreach (var m in graph.All.Where(m => !included.Contains(m.Name)))
        m.Status = Ignored;
    }
    
    public static Task<IReadOnlyCollection<GraphTaskResult>> Run(this IEnumerable<GraphTask> tasks, int parallel, ILogger log, CancellationToken cancel) =>
      Run(new TaskGraph(tasks), parallel, log, cancel);

    public static async Task<IReadOnlyCollection<GraphTaskResult>> Run(this TaskGraph tasks, int parallel, ILogger log, CancellationToken cancel) {
      async Task<GraphTaskResult> RunTask(GraphTask task) {
        var sw = Stopwatch.StartNew();

        GraphTaskResult Result(Exception ex = null) =>
          new GraphTaskResult {
            Name = task.Name,
            FinalStatus = task.Status,
            Duration = sw.Elapsed,
            Exception = ex
          };

        try {
          if (cancel.IsCancellationRequested || tasks.DependenciesDeep(task).Any(d => d.Status.In(Cancelled, Error))) {
            task.Status = Cancelled;
            return Result();
          }
          task.Status = Running;
          await task.Run(cancel);
          if (cancel.IsCancellationRequested)
            task.Status = Cancelled;
          else
            task.Status = Success;
          return Result();
        }
        catch (Exception ex) {
          task.Status = Error;
          log.Error(ex, "Task {Task} failed: {Message}", task.Name, ex.Message);
          return Result(ex);
        }
      }

      var block = new TransformBlock<GraphTask, GraphTaskResult>(RunTask,
        new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = parallel});
      var newTaskSignal = new AsyncManualResetEvent(true);

      async Task Producer() {
        while (!tasks.AllComplete) {
          if (cancel.IsCancellationRequested)
            foreach (var t in tasks.All.Where(t => t.Status.IsIncomplete()))
              t.Status = Cancelled;

          var tasksToAdd = tasks.AvailableToRun().ToList();
          if (tasksToAdd.IsEmpty()) {
            // if no tasks are ready to start. Wait to either be signaled, or log which tasks are still running
            var logTimeTask = Task.Delay(1.Minutes(), cancel);
            await Task.WhenAny(logTimeTask, newTaskSignal.WaitAsync());
            if (newTaskSignal.IsSet)
              newTaskSignal.Reset();
            if (logTimeTask.IsCompleted)
              log.Debug("Waiting for {TaskList} to complete", tasks.Running.Select(t => t.Name));
          }

          foreach (var task in tasksToAdd) {
            task.Status = Queued;
            await block.SendAsync(task);
          }
        }
        block.Complete();
      }

      var producer = Producer();

      var taskResults = new List<GraphTaskResult>();
      while (await block.OutputAvailableAsync()) {
        var item = await block.ReceiveAsync();
        taskResults.Add(item);
        newTaskSignal.Set();
      }

      await Task.WhenAll(producer, block.Completion);
      return taskResults;
    }
  }
}