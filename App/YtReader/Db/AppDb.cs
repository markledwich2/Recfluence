using System;
using System.Data.Common;
using System.Threading.Tasks;
using Mutuo.Etl.Db;
using Serilog;

namespace YtReader.Db {
  public class AppDb {
    readonly Func<Task<DbConnection>> GetConnection;

    public AppDb(Func<Task<DbConnection>> getConnection) => GetConnection = getConnection;

    public Task<DbConnection> OpenConnection() => GetConnection();
    public async Task<LoggedConnection> OpenLoggedConnection(ILogger log) => (await GetConnection()).AsLogged(log);
  }

  public class DbVideo {
    public string   VIDEO_ID      { get; set; }
    public string   VIDEO_TITLE   { get; set; }
    public string   CHANNEL_ID    { get; set; }
    public string   CHANNEL_TITLE { get; set; }
    public DateTime UPLOAD_DATE   { get; set; }
    public long     VIEWS         { get; set; }
    public string   THUMB_HIGH    { get; set; }
    public long     LIKES         { get; set; }
    public long     DISLIKES      { get; set; }
    public DateTime DURATION      { get; set; }
    public string   DESCRIPTION   { get; set; }
    public double   PCT_ADS       { get; set; }
    public DateTime UPDATED       { get; set; }
  }

  public class DbCaption {
    public string   CAPTION_ID     { get; set; }
    public string   VIDEO_ID       { get; set; }
    public string   CHANNEL_ID     { get; set; }
    public string   CAPTION        { get; set; }
    public long     OFFSET_SECONDS { get; set; }
    public DateTime UPDATED        { get; set; }
  }

  public class DbChannel {
    public string   CHANNEL_ID                            { get; set; }
    public string   CHANNEL_TITLE                         { get; set; }
    public string   MAIN_CHANNEL_ID                       { get; set; }
    public string   CHANNEL_DECRIPTION                    { get; set; }
    public string   LOGO_URL                              { get; set; }
    public string   LR                                    { get; set; }
    public long     SUBS                                  { get; set; }
    public long     CHANNEL_VIEWS                         { get; set; }
    public string   COUNTRY                               { get; set; }
    public string   TAGS                                  { get; set; }
    public DateTime UPDATED                               { get; set; }
    public long     CHANNEL_VIDEO_VIEWS                   { get; set; }
    public DateTime FROM_DATE                             { get; set; }
    public DateTime TO_DATE                               { get; set; }
    public decimal  CHANNEL_LIFETIME_DAILY_VIEWS          { get; set; }
    public double   CHANNEL_LIFETIME_DAILY_VIEWS_RELEVANT { get; set; }
    public string   MAIN_CHANNEL_TITLE                    { get; set; }
    public string   IDEOLOGY                              { get; set; }
    public string   MEDIA                                 { get; set; }
    public string   MANOEL                                { get; set; }
    public string   AIN                                   { get; set; }
  }
}