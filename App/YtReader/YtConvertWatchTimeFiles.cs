using System.Globalization;
using System.IO;
using System.Text;
using CsvHelper;
using Mutuo.Etl.Blob;
using YtReader.Store;

namespace YtReader; 

public class YtConvertWatchTimeFiles {
  readonly ISimpleFileStore Store;

  public YtConvertWatchTimeFiles(BlobStores stores) => Store = stores.Store(DataStoreType.Root);

  public async Task Convert(ILogger log) {
    var files = (await Store.List("import/watch_time").SelectManyList()).Where(f => f.Path.ExtensionsString == "csv");
    await files.BlockDo(async f => {
      using var stream = await Store.Load(f.Path);
      using var sr = new StreamReader(stream);
      using var csv = new CsvReader(sr, new(CultureInfo.InvariantCulture) {
        Encoding = Encoding.UTF8,
        HasHeaderRecord = true,
        MissingFieldFound = null,
        BadDataFound = r => log.Warning("Error reading csv data at: {RowData}", r.RawRecord)
      });
      var rows = await csv.GetRecordsAsync<dynamic>().ToListAsync();
      await Store.Save(f.Path.Parent.Add($"{f.Path.NameSansExtension}.json.gz"), await rows.ToJsonlGzStream(), log);
    }, parallel: 4);
  }
}