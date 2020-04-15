using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace YtReader.Db {
  public class AppDb {
    readonly Func<Task<DbConnection>> GetConnection;

    public AppDb(Func<Task<DbConnection>> getConnection) => GetConnection = getConnection;

    public Task<DbConnection> OpenConnection() => GetConnection();
  }

  public class DbVideo {
    public string   video_id      { get; set; }
    public string   video_title   { get; set; }
    public string   channel_id    { get; set; }
    public string   channel_title { get; set; }
    public DateTime upload_date   { get; set; }
    public string   thumb_high    { get; set; }
    public long     likes         { get; set; }
    public long     dislikes      { get; set; }
    public DateTime duration      { get; set; }
    public string   description   { get; set; }
    public double   pct_ads       { get; set; }
    public DateTime Updated       { get; set; }
  }

  public class DbCaption {
    public string   caption_id     { get; set; }
    public string   video_id       { get; set; }
    public string   channel_id     { get; set; }
    public string   caption        { get; set; }
    public long     offset_seconds { get; set; }
    public DateTime Updated        { get; set; }
  }
}