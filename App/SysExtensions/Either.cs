using JetBrains.Annotations;
using SysExtensions.Threading;

namespace SysExtensions;

/// <summary>Functional data data to represent a discriminated union of two possible types.</summary>
/// <typeparam name="T">Type of "Left" item.</typeparam>
/// <typeparam name="TAlt">Type of "Right" item.</typeparam>
public record Either<T, TAlt> : IAsyncDisposable {
  public readonly T    Main;
  public readonly TAlt Alt;
  public readonly bool IsMain;

  public Either(T main) {
    Main = main;
    IsMain = true;
  }

  public Either(TAlt alt) {
    Alt = alt;
    IsMain = false;
  }

  /*public T DoMain() => Do(l => l, _ => default);
  public TAlt DoAlt() => Do(_ => default, r => r);*/
  public static implicit operator Either<T, TAlt>(T main) => new(main);
  public static implicit operator Either<T, TAlt>(TAlt alt) => new(alt);

  public void Deconstruct(out T main, out TAlt alt) {
    main = Main;
    alt = Alt;
  }

  /// <summary>Disposes either contents if possible. Only supporting IAsyncDisposable for now</summary>
  public async ValueTask DisposeAsync() {
    if (IsMain && Main is IAsyncDisposable a) await a.DisposeAsync();
    if (!IsMain && Alt is IAsyncDisposable d) await d.DisposeAsync();
  }
}

/// <summary>Wraps Then + Either to make it succinct with async & Disposable overloads. Do/Map will disposing Either if it
///   was in a task, otherwise the caller must do it</summary>
public static class EitherExtensions {
  #region Task Based Do's

  /// <summary>run assuming main works. Will exception if alt was taken</summary>
  public static Task<T> Do<T, TAlt>(this Task<Either<T, TAlt>> either) => either.Then(e => {
    if (!e.IsMain) throw new($"Assumed main, but alt: {e.Alt}");
    return e.Main;
  }, dispose: false);

  /// <summary>awaits either, returns await of main/alt, disposes either</summary>
  public static Task<TR> Do<T, TAlt, TR>(this Task<Either<T, TAlt>> either, [NotNull] Func<T, TR> main, [NotNull] Func<TAlt, TR> alt) =>
    either.Then(e => e.IsMain ? main(e.Main) : alt(e.Alt));

  /// <summary>awaits either, awaits main/runs alt, disposes either</summary>
  public static Task Do<T, TAlt>(this Task<Either<T, TAlt>> either, [NotNull] Func<T, Task> main, [NotNull] Action<TAlt> alt) =>
    either.Then(e => e.Do(main, alt));

  /// <summary>awaits either, returns result of awaiting main/running alt, disposes either</summary>
  public static Task<TR> Do<T, TAlt, TR>(this Task<Either<T, TAlt>> either, [NotNull] Func<T, Task<TR>> main, [NotNull] Action<TAlt> alt) =>
    either.Then(e => e.Do(main, alt));

  /// <summary>awaits either, returns result of running main/alt</summary>
  public static Task<TR> Do<T, TAlt, TR>(this Task<Either<T, TAlt>> either, [NotNull] Func<T, Task<TR>> main, [NotNull] Func<TAlt, TR> alt) =>
    either.Then(e => e.Do(main, alt));

  /// <summary>awaits either, awaits main/runs alt, disposes main</summary>
  public static Task Do<T, TAlt>(this Task<Either<T, TAlt>> either, [NotNull] Func<T, Task> main, [NotNull] Func<TAlt, Task> alt) =>
    either.Then(e => e.Do(main, alt));

  /// <summary>awaits either, returns main or null</summary>
  public static Task<T> Do<T, TAlt>(this Task<Either<T, TAlt>> either, [NotNull] Action<T> main, [NotNull] Action<TAlt> alt) =>
    either.Then(e => {
      e.Do(main, alt);
      return e.Main;
    });

  /// <summary>awaits either, performs alt appropriately</summary>
  public static Task<T> DoAlt<T, TAlt>(this Task<Either<T, TAlt>> either, [NotNull] Action<TAlt> alt) =>
    either.Then(e => {
      e.Do(_ => { }, alt);
      return e.Main;
    });

  #endregion

  #region Do's

  /// <summary>runs main/alt</summary>
  public static void Do<T, TAlt>(this Either<T, TAlt> either, [NotNull] Action<T> main, [NotNull] Action<TAlt> alt) {
    if (either.IsMain) main(either.Main);
    else alt(either.Alt);
  }

  /// <summary>returns result of main/alt</summary>
  public static Task Do<T, TAlt>(this Either<T, TAlt> either, [NotNull] Func<T, Task> main, [NotNull] Func<TAlt, Task> alt) =>
    either.IsMain ? main(either.Main) : alt(either.Alt);

  /// <summary>returns await of main/alt</summary>
  public static async Task<TR> Do<T, TAlt, TR>(this Either<T, TAlt> either, [NotNull] Func<T, Task<TR>> main, [NotNull] Func<TAlt, TR> alt) =>
    either.IsMain ? await main(either.Main) : alt(either.Alt);

  /// <summary>returns await of main, runs alt</summary>
  public static async Task<TR> Do<T, TAlt, TR>(this Either<T, TAlt> either, [NotNull] Func<T, Task<TR>> main, [NotNull] Action<TAlt> alt) {
    if (either.IsMain) return await main(either.Main);
    alt(either.Alt);
    return default;
  }

  /// <summary>returns await of main/run of alt</summary>
  public static async Task Do<T, TAlt>(this Either<T, TAlt> either, [NotNull] Func<T, Task> main, [NotNull] Action<TAlt> alt) {
    if (either.IsMain) await main(either.Main);
    else alt(either.Alt);
  }

  /// <summary>runs main/awaits alt</summary>
  public static async Task Do<T, TAlt>(this Either<T, TAlt> either, [NotNull] Action<T> main, [NotNull] Func<TAlt, Task> alt) {
    if (either.IsMain) main(either.Main);
    else await alt(either.Alt);
  }

  #endregion
}