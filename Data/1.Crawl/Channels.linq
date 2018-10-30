<Query Kind="Statements">
  <Reference Relative="..\..\..\..\.nuget\packages\litedb\4.1.4\lib\net40\LiteDB.dll">C:\Users\mark\.nuget\packages\litedb\4.1.4\lib\net40\LiteDB.dll</Reference>
  <Reference Relative="..\..\Crawler\YouTubeReader\bin\Debug\netcoreapp2.1\YouTubeReader.dll">C:\Users\mark\Repos\YouTubeNetworks\Crawler\YouTubeReader\bin\Debug\netcoreapp2.1\YouTubeReader.dll</Reference>
  <NuGetReference>Newtonsoft.Json</NuGetReference>
  <NuGetReference>Serilog</NuGetReference>
  <Namespace>YouTubeReader</Namespace>
</Query>

var db = Setup.Db();

var cvs = db.ChannelVideos();

//cvs.Count().Dump();
//db.Channels().Count().Dump();

/*foreach(var cv in cvs.Find(c => c.ChannelTitle == null)) {
	var title = db.Channels().FindById(cv.ChannelId).Title;
	cv.ChannelTitle = title;
	cvs.Update(cv);
}*/

/*
var title = "Philip DeFranco";
db.Channels().FindOne(c => c.Title == title).Dump();
cvs.FindOne(c => c.ChannelTitle == title).Dump();
db.Videos().Find(v => v.ChannelTitle == title).Dump();*/


db.Channels().FindAll().Select(c => new { c.Title, Url=$"https://youtube.com/channel/{c.Id}", c.ViewCount}).OrderByDescending(c => c.ViewCount).Dump();

//db.VideoRecommended().FindAll().Dump(); //.Find(d => d.VideoId == "7ZxhLli-7CY").Dump();


