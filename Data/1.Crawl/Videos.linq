<Query Kind="Statements">
  <Reference Relative="..\..\..\..\.nuget\packages\litedb\4.1.4\lib\net40\LiteDB.dll">C:\Users\mark\.nuget\packages\litedb\4.1.4\lib\net40\LiteDB.dll</Reference>
  <Reference Relative="..\..\Crawler\YouTubeReader\bin\Debug\netcoreapp2.1\YouTubeReader.dll">C:\Users\mark\Repos\YouTubeNetworks\Crawler\YouTubeReader\bin\Debug\netcoreapp2.1\YouTubeReader.dll</Reference>
  <Namespace>YouTubeReader</Namespace>
</Query>

var db = Setup.Db();
var videos = db.Videos();
//videos.Find(q => q.Video.ChannelTitle.StartsWith("The Young Turks")).Select(q => new { q.Id, q.Video.Title, q.Video.PublishedAt }).Dump();
videos.Count().Dump();

//videos.Find(v => v.ChannelTitle == "LastWeekTonight").Dump();