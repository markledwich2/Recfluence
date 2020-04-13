using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;

namespace Mutuo.Etl.Pipe {
  public static class AzureEx {
    public static IAzure GetAzure(this PipeAzureCfg cfg) {
      var sp = cfg.ServicePrincipal;
      var creds = new AzureCredentialsFactory().FromServicePrincipal(sp.ClientId, sp.Secret, sp.TennantId, AzureEnvironment.AzureGlobalCloud);
      var azure = Azure.Authenticate(creds).WithSubscription(cfg.SubscriptionId);
      return azure;
    }
  }
}