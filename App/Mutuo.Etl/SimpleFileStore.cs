using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using SysExtensions.Fluent.IO;
using SysExtensions.Text;

namespace Mutuo.Etl {
  public interface ISimpleFileStore {
    Task<T> Get<T>(StringPath path) where T : class;
    Task Set<T>(StringPath path, T item);
    Task Save(StringPath path, FPath file);
    Task Save(StringPath path, Stream contents);
    Task<Stream> Load(StringPath path);
    IAsyncEnumerable<IReadOnlyCollection<FileListItem>> List(StringPath path, bool allDirectories = false);
    Task<bool> Delete(StringPath path);
    Task<Stream> OpenForWrite(StringPath path, FileProps props = null);
  }
  
  public static class SimpleStoreExtensions {
    public static async Task<T> GetOrCreate<T>(this ISimpleFileStore store, StringPath path, Func<T> create = null) where T : class, new() {
      var o = await store.Get<T>(path);
      if (o == null) {
        o = create == null ? new T() : create();
        await store.Set(path, o);
      }
      return o;
    }
  }
}