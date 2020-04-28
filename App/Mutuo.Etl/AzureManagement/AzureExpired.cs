using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Mutuo.Etl.Pipe;
using Serilog;
using SysExtensions.Text;

namespace Mutuo.Etl.AzureManagement {
  public static class AzureExpired {
    public static (string key, string value) ExpireTag(DateTime utcDate) => ("expire", utcDate.ToString("o", DateTimeFormatInfo.InvariantInfo));

    static bool IsExpired(this IResource resource) {
      if (resource.Tags.TryGetValue("expire", out var expire))
        return expire.ParseDate(DateTimeFormatInfo.InvariantInfo, DateTimeStyles.RoundtripKind) < DateTime.UtcNow;
      return false;
    }

    public static async Task DeleteExpiredResources(this IAzure azure, ILogger log) {
      var allGroups = await azure.ContainerGroups.ListAsync();
      var toDelete = allGroups.Where(g => g.IsExpired() && g.State().IsCompletedState()).ToArray();
      if (toDelete.Any()) {
        await azure.ContainerGroups.DeleteByIdsAsync(toDelete.Select(g => g.Id).ToArray());
        log.Information("Deleted expired container groups: {@toDelete}", toDelete.Select(g => g.Name).ToArray());
      }
      else {
        log.Debug("No expired container groups to delete");
      }
    }
  }
}