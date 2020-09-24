using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using SysExtensions.Collections;

namespace Mutuo.Etl.Db {
  public class SyncTableCfg {
    public SyncTableCfg(string name, params SyncColCfg[] cols) {
      Name = name;
      Cols.AddRange(cols);
    }

    public SyncTableCfg() { }

    public            SyncType SyncType { get; set; }
    [Required] public string   Name     { get; set; }

    [Required]
    public IKeyedCollection<string, SyncColCfg> Cols { get; set; } =
      new KeyedCollection<string, SyncColCfg>(c => c.Name, StringComparer.InvariantCultureIgnoreCase);

    public string[] SelectedCols { get; set; } = { };

    /// <summary>when true, the sync process won't change the destination schema</summary>
    public bool ManualSchema { get; set; }

    /// <summary>an SQL filter to limit sync</summary>
    public string Filter { get; set; }

    public bool ColStore { get; set; } = true;

    public string   TsCol  => Cols.FirstOrDefault(c => c.Ts)?.Name;
    public string[] IdCols => Cols.Where(c => c.Id).Select(c => c.Name).ToArray();
    public string   Sql    { get; set; }
  }

  public enum SyncType {
    Incremental,
    Full
  }

  public class SyncColCfg {
    public SyncColCfg(string name) => Name = name;

    public string Name     { get; set; }
    public bool   Ts       { get; set; }
    public bool   Id       { get; set; }
    public string SqlType  { get; set; }
    public bool   Null     { get; set; } = true;
    public bool   Index    { get; set; }
    public bool   FullText { get; set; }
  }
}