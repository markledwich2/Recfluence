using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace SysExtensions.Threading
{
    public static class BlockExtensions
    {
        public static async Task BlockAction<T>(this IEnumerable<T> source, Func<T, Task> action, int parallelism = 1, int? capacity = null)
        {
            var options = new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = parallelism};
            if (capacity.HasValue) options.BoundedCapacity = capacity.Value;

            var block = new ActionBlock<T>(action, options);
            var produce = Produce(source, block);
            await Task.WhenAll(produce, block.Completion);
        }

        /// <summary>
        ///     Simplified method for async operations that don't need to be chained, and when the result can fit in memory
        /// </summary>
        public static async Task<IReadOnlyCollection<R>> BlockTransform<T, R>(this IEnumerable<T> source,
            Func<T, Task<R>> transform,
            int parallelism = 1, int? capacity = null, Action<List<R>> progressUpdate = null)
        {
            var options = new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = parallelism};
            if (capacity.HasValue) options.BoundedCapacity = capacity.Value;
            var block = new TransformBlock<T, R>(transform, options);

            // by producing asynchronously and using SendAsync we can throttle how much we can form the source and consume at the same time
            var produce = Produce(source, block);

            var result = new List<R>();
            while (await block.OutputAvailableAsync())
            {
                var item = await block.ReceiveAsync();
                result.Add(item);
                progressUpdate?.Invoke(result);
            }

            await Task.WhenAll(produce, block.Completion);
            return result;
        }

        static async Task Produce<T>(IEnumerable<T> source, ITargetBlock<T> block)
        {
            foreach (var item in source)
                await block.SendAsync(item);
            block.Complete();
        }
    }
}