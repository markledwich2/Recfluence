using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Blob;
using SysExtensions.Text;

namespace Mutuo.Etl.Blob {
  public static class AzureBlobExtensions {
    public static async Task<string> LoadAsText(this CloudBlobClient client, string containerName, string blob) {
      var container = client.GetContainerReference(containerName);
      var blobRef = container.GetBlobReference(blob);
      return await LoadAsText(blobRef);
    }

    public static async Task<string> LoadAsText(this CloudBlob blobRef) {
      using var memoryStream = new MemoryStream();
      await blobRef.DownloadToStreamAsync(memoryStream);
      var text = memoryStream.ToArray().ToStringFromUtf8();
      return text;
    }
  }
}