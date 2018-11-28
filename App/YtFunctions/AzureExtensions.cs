using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using SysExtensions.Text;

namespace YtFunctions {
    public static class AzureExtensions {

        public static async Task<string> GetText(this CloudBlobClient client, string containerName, string blob) {
            var container = client.GetContainerReference(containerName);
            var blobRef = container.GetBlobReference(blob);
            string text;
            using (var memoryStream = new MemoryStream()) {
                await blobRef.DownloadToStreamAsync(memoryStream);
                text = memoryStream.ToArray().ToStringFromUtf8();
            }
            return text;
        } 

    }
}
