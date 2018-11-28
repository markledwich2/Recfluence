using System;
using System.Collections.Specialized;
using System.Globalization;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Web;
using Microsoft.WindowsAzure.Storage;
using SysExtensions.Text;
using System.Linq;

namespace YtReader {
    public static class AzureBlobHelper {

        public static Uri BlobUri(this CloudStorageAccount storage, StringPath path) => new Uri($"{storage.BlobEndpoint}{path}");

        public static HttpRequestMessage WithBlobHeaders(this HttpRequestMessage req, CloudStorageAccount storage) {
            
            var creds = storage.Credentials;

            //var request = new HttpRequestMessage(method, $"{Storage.BlobEndpoint}{ContainerName}/{BasePath}/{path}");
            var now = DateTime.UtcNow;
            req.Headers.Add("x-ms-date", now.ToString("R", CultureInfo.InvariantCulture));
            req.Headers.Add("x-ms-version", "2017-07-29");
            req.Headers.Add("x-ms-blob-type", "BlockBlob");
            
            req.Headers.Authorization = GetAuthorizationHeader(creds.AccountName, creds.ExportBase64EncodedKey(), now, req);
            return req;
        }

        private static string GetCanonicalizedResource(Uri address, string storageAccountName) {
            // The absolute path will be "/" because for we're getting a list of containers.
            StringBuilder sb = new StringBuilder("/").Append(storageAccountName).Append(address.AbsolutePath);

            // Address.Query is the resource, such as "?comp=list".
            // This ends up with a NameValueCollection with 1 entry having key=comp, value=list.
            // It will have more entries if you have more query parameters.
            NameValueCollection values = HttpUtility.ParseQueryString(address.Query);

            foreach (var item in values.AllKeys.OrderBy(k => k)) {
                sb.Append('\n').Append(item).Append(':').Append(values[item]);
            }

            return sb.ToString();
        }

        private static AuthenticationHeaderValue GetAuthorizationHeader(string storageAccountName, string storageAccountKey, DateTime now,
            HttpRequestMessage httpRequestMessage, string ifMatch = "", string md5 = "") {
            // This is the raw representation of the message signature.
            var method = httpRequestMessage.Method;
            var MessageSignature = String.Format("{0}\n\n\n{1}\n{5}\n\n\n\n{2}\n\n\n\n{3}{4}",
                        method.ToString(),
                        (method == HttpMethod.Get || method == HttpMethod.Head) ? String.Empty
                          : httpRequestMessage.Content.Headers.ContentLength.ToString(),
                        ifMatch,
                        GetCanonicalizedHeaders(httpRequestMessage),
                        GetCanonicalizedResource(httpRequestMessage.RequestUri, storageAccountName),
                        md5);

            // Now turn it into a byte array.
            var SignatureBytes = Encoding.UTF8.GetBytes(MessageSignature);

            // Create the HMACSHA256 version of the storage key.
            var SHA256 = new HMACSHA256(Convert.FromBase64String(storageAccountKey));

            // Compute the hash of the SignatureBytes and convert it to a base64 string.
            string signature = Convert.ToBase64String(SHA256.ComputeHash(SignatureBytes));

            // This is the actual header that will be added to the list of request headers.
            var authHV = new AuthenticationHeaderValue("SharedKey",
                storageAccountName + ":" + Convert.ToBase64String(SHA256.ComputeHash(SignatureBytes)));
            return authHV;
        }

        private static string GetCanonicalizedHeaders(HttpRequestMessage httpRequestMessage) {
            var headers = from kvp in httpRequestMessage.Headers
                          where kvp.Key.StartsWith("x-ms-", StringComparison.OrdinalIgnoreCase)
                          orderby kvp.Key
                          select new { Key = kvp.Key.ToLowerInvariant(), kvp.Value };

            StringBuilder sb = new StringBuilder();

            // Create the string in the right format; this is what makes the headers "canonicalized" --
            //   it means put in a standard format. http://en.wikipedia.org/wiki/Canonicalization
            foreach (var kvp in headers) {
                StringBuilder headerBuilder = new StringBuilder(kvp.Key);
                char separator = ':';

                // Get the value for each header, strip out \r\n if found, then append it with the key.
                foreach (string headerValues in kvp.Value) {
                    string trimmedValue = headerValues.TrimStart().Replace("\r\n", String.Empty);
                    headerBuilder.Append(separator).Append(trimmedValue);

                    // Set this to a comma; this will only be used 
                    //   if there are multiple values for one of the headers.
                    separator = ',';
                }
                sb.Append(headerBuilder.ToString()).Append("\n");
            }
            return sb.ToString();
        }

    }
}