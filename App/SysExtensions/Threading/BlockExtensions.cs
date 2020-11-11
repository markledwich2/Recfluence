using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Humanizer;
using Newtonsoft.Json;
using SysExtensions.Collections;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;

namespace SysExtensions.Threading {
  public static class BlockExtensions {
    public static async Task<long> BlockAction<T>(this IEnumerable<T> source, Func<T, Task> action, int parallel = 1, int? capacity = null,
      CancellationToken cancel = default) {
      var options = new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = parallel, EnsureOrdered = false, CancellationToken = cancel};
      if (capacity.HasValue) options.BoundedCapacity = capacity.Value;

      var block = new ActionBlock<T>(action, options);
      var produced = await ProduceAsync(source, block);
      await block.Completion;
      return produced;
    }

    /// <summary>Uses the type context of an enumerable to make a block, but does not touch it.</summary>
    public static (IEnumerable<T> source, IPropagatorBlock<T, R> first, IPropagatorBlock<T, R> last) BlockFuncWith<T, R>
      (this IEnumerable<T> source, Func<T, Task<R>> transform, int parallelism = 1, int? capacity = null) {
      var options = new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = parallelism, EnsureOrdered = false};
      if (capacity.HasValue) options.BoundedCapacity = capacity.Value;
      var trans = new TransformBlock<T, R>(transform, options);
      return (source, trans, trans);
    }

    public static (IEnumerable<T> source, IPropagatorBlock<T, TFirstR> first, IPropagatorBlock<TLastR, R> last) Then<T, TFirstR, TLast, TLastR, R>(
      this (IEnumerable<T> source, IPropagatorBlock<T, TFirstR> first, IPropagatorBlock<TLast, TLastR> last) withFunc, Func<TLastR, Task<R>> transform,
      int parallelism = 1, int? capacity = null) {
      var options = new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = parallelism, EnsureOrdered = false};
      if (capacity.HasValue) options.BoundedCapacity = capacity.Value;
      var last = new TransformBlock<TLastR, R>(transform, options);
      withFunc.last.LinkTo(last, new DataflowLinkOptions {PropagateCompletion = true});
      return (withFunc.source, withFunc.first, last);
    }

    public static async Task<IReadOnlyCollection<TLastR>> Run<T, TFirstR, TLast, TLastR>(
      this (IEnumerable<T> source, IPropagatorBlock<T, TFirstR> first, IPropagatorBlock<TLast, TLastR> last) withFunc) {
      var (source, first, last) = withFunc;
      var produced = source.ProduceAsync(first);
      var consume = last.ConsumeAsync();
      await produced;
      await first.Completion;
      await last.Completion;
      var res = await consume;
      return res;
    }

    public static async Task<int> BlockBatch<T>(this IEnumerable<T> source,
      Func<IReadOnlyCollection<T>, Task> action, int batchSize = 10_000, int parallel = 1, int fileParallel = 4,
      JsonSerializerSettings serializerSettings = null) {
      var res = await source.BlockBatch(async (b, i) => {
        await action(b).ConfigureAwait(false);
        return b.Count;
      }, batchSize, parallel, fileParallel, serializerSettings);
      return res.Sum();
    }

    public static async Task<int> BlockBatch<T>(this IEnumerable<T> source,
      Func<IReadOnlyCollection<T>, int, Task> action, int batchSize = 10_000, int parallel = 1, int fileParallel = 4,
      JsonSerializerSettings serializerSettings = null) {
      var res = await source.BlockBatch(async (b, i) => {
        await action(b, i).ConfigureAwait(false);
        return b.Count;
      }, batchSize, parallel, fileParallel, serializerSettings);
      return res.Sum();
    }

    /// <summary>Batches the source and uses temporary files to avoid memory usage. To avoid the overhead of streaming objects
    ///   though, they are batched. ensure oyu can fit batchSize objects * max(4, parallel) in memory. This returns all result
    ///   of the transform in memory, so don't return items from transform</summary>
    public static async Task<IReadOnlyCollection<R>> BlockBatch<T, R>(this IEnumerable<T> source,
      Func<IReadOnlyCollection<T>, int, Task<R>> transform, int batchSize = 10_000, int parallel = 1, int fileParallel = 4,
      JsonSerializerSettings serializerSettings = null) {
      var id = Guid.NewGuid().ToShortString();
      var dir = "BlockFuncLarge".AsPath().InAppData("Mutuo.Etl");
      dir.EnsureDirectoryExists();
      return await source
        .Batch(batchSize)
        .WithIndex()
        .BlockFuncWith(async b => {
          var (batch, i) = b;
          var file = dir.Combine($"{id}.{i}.json.gz");
          await batch.ToJsonlGz(file.FullPath, serializerSettings);
          return (file, i);
        }, fileParallel, fileParallel * 2)
        .Then(async f => {
          IReadOnlyCollection<T> items;
          using (var stream = f.file.Open(FileMode.Open)) items = stream.LoadJsonlGz<T>();
          f.file.Delete();
          return await transform(items, f.i).ConfigureAwait(false);
        }, parallel)
        .Run().ConfigureAwait(false);
    }

    public static async IAsyncEnumerable<R> BlockTrans<T, R>(this IEnumerable<T> source,
      Func<T, Task<R>> func, int parallel = 1, int? capacity = null, [EnumeratorCancellation] CancellationToken cancel = default) {
      var block = GetBlock(func, parallel, capacity, cancel);
      var produceTask = ProduceAsync(source, block);
      while (true) {
        if(produceTask.IsFaulted) {
          block.Complete();
          break;
        }
        if (!await block.OutputAvailableAsync()) break;
        yield return await block.ReceiveAsync();
      }
      await Task.WhenAll(produceTask, block.Completion);
    }

    public static async IAsyncEnumerable<R> BlockTrans<T, R>(this IAsyncEnumerable<T> source,
      Func<T, Task<R>> func, int parallel = 1, int? capacity = null, [EnumeratorCancellation] CancellationToken cancel = default) {
      var block = GetBlock(func, parallel, capacity, cancel);
      var produceTask = ProduceAsync(source, block, cancel);
      while (true) {
        if(produceTask.IsFaulted) {
          block.Complete();
          break;
        }
        if (!await block.OutputAvailableAsync()) break;
        yield return await block.ReceiveAsync();
      }
      await Task.WhenAll(produceTask, block.Completion);
    }

    public static async IAsyncEnumerable<R> BlockTrans<T, R>(this Task<IAsyncEnumerable<T>> source,
      Func<T, Task<R>> func, int parallel = 1, int? capacity = null, [EnumeratorCancellation] CancellationToken cancel = default) {
      await foreach (var i in (await source).BlockTrans(func, parallel, capacity, cancel))
        yield return i;
    }

    static TransformBlock<T, R> GetBlock<T, R>(Func<T, Task<R>> func, int parallel = 1, int? capacity = null, CancellationToken cancel = default) {
      var options = new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = parallel, EnsureOrdered = false, CancellationToken = cancel};
      if (capacity.HasValue) options.BoundedCapacity = capacity.Value;
      return new TransformBlock<T, R>(func, options);
    }

    /// <summary>Simplified method for async operations that don't need to be chained, and when the result can fit in memory</summary>
    public static async Task<IReadOnlyCollection<R>> BlockFunc<T, R>(this IEnumerable<T> source,
      Func<T, Task<R>> func, int parallel = 1, int? capacity = null,
      Action<BulkProgressInfo> progressUpdate = null, TimeSpan progressPeriod = default, CancellationToken cancel = default) {
      progressPeriod = progressPeriod == default ? 60.Seconds() : progressPeriod;
      var options = new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = parallel, EnsureOrdered = false, CancellationToken = cancel};
      if (capacity.HasValue) options.BoundedCapacity = capacity.Value;
      var block = new TransformBlock<T, R>(func, options);

      var swProgress = Stopwatch.StartNew();

      // by producing asynchronously and using SendAsync we can throttle how much we can form the source and consume at the same time
      var produceTask = ProduceAsync(source, block);
      var result = new List<R>();
      var newResults = new List<R>();
      while (true) {
        if (produceTask.IsFaulted)
          break;

        var outputAvailableTask = block.OutputAvailableAsync();
        var completedTask = await Task.WhenAny(outputAvailableTask, Task.Delay(progressPeriod));
        if (completedTask == outputAvailableTask) {
          var available = await outputAvailableTask;
          if (!available)
            break;
          var item = await block.ReceiveAsync();
          newResults.Add(item);
          result.Add(item);
        }

        var elapsed = swProgress.Elapsed;
        if (elapsed > progressPeriod) {
          progressUpdate?.Invoke(new BulkProgressInfo(newResults.Count, result.Count, elapsed));
          swProgress.Restart();
          newResults.Clear();
        }
      }

      await produceTask;
      await block.Completion;

      return result;
    }

    static async Task<long> ProduceAsync<T>(this IAsyncEnumerable<T> source, ITargetBlock<T> block, CancellationToken cancel = default) {
      var produced = 0;
      await foreach (var item in source) {
        if (cancel.IsCancellationRequested) return produced;
        await block.SendAsync(item).ConfigureAwait(false);
        produced++;
      }
      block.Complete();
      return produced;
    }

    static async Task<long> ProduceAsync<T>(this IEnumerable<T> source, ITargetBlock<T> block) {
      var produced = 0;
      foreach (var item in source) {
        await block.SendAsync(item).ConfigureAwait(false);
        produced++;
      }
      block.Complete();
      return produced;
    }

    static async Task<IReadOnlyCollection<T>> ConsumeAsync<T>(this ISourceBlock<T> block) {
      var list = new List<T>();
      while (await block.OutputAvailableAsync())
        list.Add(await block.ReceiveAsync());
      return list;
    }
  }

  public class BulkProgressInfo {
    public BulkProgressInfo(int completed, int completedTotal, TimeSpan elapsed) {
      Completed = completed;
      CompletedTotal = completedTotal;
      Elapsed = elapsed;
    }

    public int      Completed      { get; }
    public int      CompletedTotal { get; }
    public TimeSpan Elapsed        { get; }

    public Speed Speed(string units) => Completed.Speed(units, Elapsed);
  }
}