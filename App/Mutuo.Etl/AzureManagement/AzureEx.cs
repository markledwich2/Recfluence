using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Mutuo.Etl.Pipe;

namespace Mutuo.Etl.AzureManagement {
  public static class AzureEx {
    public static IAzure GetAzure(this PipeAzureCfg cfg) {
      var sp = cfg.ServicePrincipal;
      var creds = new AzureCredentialsFactory().FromServicePrincipal(sp.ClientId, sp.Secret, sp.TennantId, AzureEnvironment.AzureGlobalCloud);
      var azure = Azure.Authenticate(creds).WithSubscription(cfg.SubscriptionId);
      return azure;
    }
  }
}