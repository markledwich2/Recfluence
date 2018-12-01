using System.Threading.Tasks;
using SysExtensions.Fluent.IO;
using SysExtensions.Text;

namespace YtReader {
    public interface ISimpleFileStore {
        Task<T> Get<T>(StringPath path) where T : class;
        Task Set<T>(StringPath path, T item);
        Task Save(StringPath path, FPath file);
    }
}
