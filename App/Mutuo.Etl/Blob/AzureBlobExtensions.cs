using System.IO;
using Azure.Storage.Blobs;

namespace Mutuo.Etl.Blob;

public static class AzureBlobExtensions {
  public static async Task<string> LoadAsText(this BlobClient blobClient) {
    using var memoryStream = new MemoryStream();
    await blobClient.DownloadToAsync(memoryStream);
    var text = memoryStream.ToArray().ToStringFromUtf8();
    return text;
  }
}