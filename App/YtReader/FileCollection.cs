using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;

namespace YtReader {
  public class FileCollection<T> where T : class {
    public FileCollection(ISimpleFileStore s3, Expression<Func<T, string>> getId, StringPath path,
      CollectionCacheType cacheType = CollectionCacheType.Memory,
      FPath localCacheDir = null) {
      Store = s3;
      GetId = getId.Compile();
      Path = path;
      CacheType = cacheType;
      LocalCacheDir = localCacheDir;
      Cache = new KeyedCollection<string, T>(getId, theadSafe: true);
    }

    ISimpleFileStore Store { get; }
    Func<T, string> GetId { get; }
    StringPath Path { get; }
    CollectionCacheType CacheType { get; }
    FPath LocalCacheDir { get; }
    IKeyedCollection<string, T> Cache { get; }

    T GetFromCache(string id) {
      switch (CacheType) {
        case CollectionCacheType.None:
          return null;
        case CollectionCacheType.Memory:
        case CollectionCacheType.MemoryAndDisk:
          var item = Cache[id];
          if (item != null) return item;
          if (CacheType == CollectionCacheType.MemoryAndDisk && LocalCacheDir != null) {
            var file = GetFilePath(id);
            if (file.Exists)
              return file.ToObject<T>();
          }

          break;
      }

      return null;
    }

    void SetCache(string id, T item) {
      if (item == null) return;
      switch (CacheType) {
        case CollectionCacheType.Memory:
        case CollectionCacheType.MemoryAndDisk:
          Cache.Add(item);
          if (CacheType == CollectionCacheType.MemoryAndDisk && LocalCacheDir != null) {
            var file = GetFilePath(id);

            if (!file.Parent().Exists)
              file.Parent().EnsureDirectoryExists();

            item.ToJsonFile(file);
          }

          break;
      }
    }

    FPath GetFilePath(string id) => LocalCacheDir.Combine(Path.Add($"{id}.json").Tokens.ToArray());

    public async Task<T> Get(string id) {
      var o = GetFromCache(id);
      if (o != null)
        return o;
      o = await Store.Get<T>(Path.Add(id));
      SetCache(id, o);
      return o;
    }

    public async Task<T> GetOrCreate(string id, Func<string, Task<T>> create) {
      var o = GetFromCache(id);
      if (o != null)
        return o;

      o = await Store.Get<T>(Path.Add(id));
      var missingFromS3 = o == null;
      if (missingFromS3)
        o = await create(id);

      if (o == null)
        return null;

      if (missingFromS3)
        await Store.Set(Path.Add(id), o);

      SetCache(id, o);
      return o;
    }

    public async Task<T> Set(T item) {
      await Store.Set(Path.Add(GetId(item)), item);
      SetCache(GetId(item), item);
      return item;
    }
  }

  public enum CollectionCacheType {
    None,
    Memory,
    MemoryAndDisk // useful for local caches during development
  }
}