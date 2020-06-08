using System;
using System.Threading.Tasks;
using SysExtensions.Text;

namespace Mutuo.Etl.Blob {
  /// <summary>Ready/write to storage for a keyed collection of items</summary>
  /// <typeparam name="T"></typeparam>
  public class KeyedCollectionStore<T> where T : class {
    public KeyedCollectionStore(ISimpleFileStore store, Func<T, string> getId, StringPath path) {
      Store = store;
      GetId = getId;
      Path = path;
    }

    ISimpleFileStore Store { get; }
    Func<T, string>  GetId { get; }
    StringPath       Path  { get; }

    public async Task<T> Get(string id) => await Store.Get<T>(Path.Add(id));
    public async Task Set(T item) => await Store.Set(Path.Add(GetId(item)), item);
  }
}