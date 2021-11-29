using Newtonsoft.Json.Linq;
using YtReader.Store;

namespace YtReader.Yt; 

public enum AgoUnit {
  Minute,
  Hour,
  Day,
  Week,
  Month,
  Year
}

public static class YtWebExtensions {
  public static (TimeSpan Dur, AgoUnit Unit) ParseAgo(this string ago) {
    if (ago == null) return default;
    var res = Regex.Match(ago, "(?<num>\\d+)\\s(?<unit>minute|hour|day|week|month|year)[s]? ago");
    if (!res.Success) return default;
    var num = res.Groups["num"].Value.ParseInt();
    var unit = res.Groups["unit"].Value.ParseEnum<AgoUnit>();
    var timeSpan = unit switch {
      AgoUnit.Minute => num.Minutes(),
      AgoUnit.Hour => num.Hours(),
      AgoUnit.Day => num.Days(),
      AgoUnit.Week => num.Weeks(),
      AgoUnit.Month => TimeSpan.FromDays(365 / 12.0 * num),
      AgoUnit.Year => TimeSpan.FromDays(365 * num),
      _ => throw new InvalidOperationException($"unexpected ago unit {res.Groups["unit"].Value}")
    };
    return (timeSpan, unit);
  }

  public static DateTime Date(this (TimeSpan Dur, AgoUnit Unit) ago) => ago.Dur.Before(DateTime.UtcNow);

  public static long? ParseViews(this string s) {
    if (s.NullOrEmpty()) return null;
    var m = Regex.Match(s, "^(\\d+,?\\d*) views");
    if (!m.Success) return null;
    var views = m.Groups[1].Value.ParseLong();
    return views;
  }

  public static bool ParseForYou(string viewText) => viewText == "Recommended for you";

  static readonly Regex SubRegex = new("(?'num'\\d+\\.?\\d*)(?'unit'[BMK]?)", RegexOptions.Compiled);

  public static long? ParseSubs(this string s) {
    if (s.NullOrEmpty()) return null;
    var m = SubRegex.Match(s);
    var subs = m.Groups["num"].Value.ParseDecimal();
    var multiplier = m.Groups["unit"].Value switch {
      "K" => 1_000,
      "M" => 1_000_000,
      "B" => 1_000_000_000,
      _ => 1
    };
    return (long) Math.Round(subs * multiplier);
  }

  /// <summary>Finds the text for a path. Either from simpleText or runs[0].text</summary>
  public static string YtTxt(this JToken j, string path = null) {
    j = path == null ? j : j.Token(path);
    return j?.Str("simpleText") ?? j?.Token("runs[0].text")?.Str();
  }

  public static (VideoExtra[] Extras, Rec[] Recs, VideoComment[] Comments, VideoCaption[] Captions) Split(this IReadOnlyCollection<ExtraAndParts> extras) =>
    (extras.Extras().ToArray(), extras.Recs().ToArray(), extras.Comments().ToArray(), extras.Captions().ToArray());

  public static IEnumerable<VideoExtra> Extras(this IEnumerable<ExtraAndParts> extra) => extra.Select(e => e.Extra).NotNull();
  public static IEnumerable<Rec> Recs(this IEnumerable<ExtraAndParts> extra) => extra.SelectMany(e => e.Recs).NotNull();
  public static IEnumerable<VideoComment> Comments(this IEnumerable<ExtraAndParts> extra) => extra.SelectMany(e => e.Comments).NotNull();
  public static IEnumerable<VideoCaption> Captions(this IEnumerable<ExtraAndParts> extra) => extra.Select(e => e.Caption).NotNull();

  /// <summary>Verifies that the given string is syntactically a valid YouTube channel ID.</summary>
  public static bool ValidateChannelId(string channelId) {
    if (channelId.IsNullOrWhiteSpace())
      return false;

    // Channel IDs should start with these characters
    if (!channelId.StartsWith("UC", StringComparison.Ordinal))
      return false;

    // Channel IDs are always 24 characters
    if (channelId.Length != 24)
      return false;

    return !Regex.IsMatch(channelId, @"[^0-9a-zA-Z_\-]");
  }

  public static bool ValidateVideoId(string videoId) {
    if (videoId.IsNullOrWhiteSpace())
      return false;

    // Video IDs are always 11 characters
    if (videoId.Length != 11)
      return false;

    return !Regex.IsMatch(videoId, @"[^0-9a-zA-Z_\-]");
  }

  /// <summary>Parses video ID from a YouTube video URL.</summary>
  public static string ParseVideoId(string videoUrl) =>
    TryParseVideoId(videoUrl, out var result)
      ? result
      : throw new FormatException($"Could not parse video ID from given string [{videoUrl}].");

  /// <summary>Tries to parse video ID from a YouTube video URL.</summary>
  public static bool TryParseVideoId(string videoUrl, out string videoId) {
    videoId = default;

    if (videoUrl.IsNullOrWhiteSpace())
      return false;

    // https://www.youtube.com/watch?v=yIVRs6YSbOM
    var regularMatch = Regex.Match(videoUrl, @"youtube\..+?/watch.*?v=(.*?)(?:&|/|$)").Groups[1].Value;
    if (!regularMatch.IsNullOrWhiteSpace() && ValidateVideoId(regularMatch)) {
      videoId = regularMatch;
      return true;
    }

    // https://youtu.be/yIVRs6YSbOM
    var shortMatch = Regex.Match(videoUrl, @"youtu\.be/(.*?)(?:\?|&|/|$)").Groups[1].Value;
    if (!shortMatch.IsNullOrWhiteSpace() && ValidateVideoId(shortMatch)) {
      videoId = shortMatch;
      return true;
    }

    // https://www.youtube.com/embed/yIVRs6YSbOM
    var embedMatch = Regex.Match(videoUrl, @"youtube\..+?/embed/(.*?)(?:\?|&|/|$)").Groups[1].Value;
    if (!embedMatch.IsNullOrWhiteSpace() && ValidateVideoId(embedMatch)) {
      videoId = embedMatch;
      return true;
    }

    return false;
  }
}