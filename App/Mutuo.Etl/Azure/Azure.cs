using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Azure.Management.ContainerInstance.Fluent;
using Microsoft.Azure.Management.Graph.RBAC.Fluent;
using Microsoft.Azure.Management.Msi.Fluent;
using Microsoft.Azure.Management.Network.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Azure.Management.Storage.Fluent;
using Microsoft.Rest.Azure;

namespace Mutuo.Etl.Pipe {
  /// <summary>Convenience methods for azure. Taken from
  ///   https://github.com/Azure/azure-libraries-for-net/tree/master/src/ResourceManagement/Azure.Fluent The problem with the
  ///   fluent package is that it depends on ALL of them</summary>
  public class Azure : IAzure {
    readonly IAuthenticated authenticated;

    readonly IContainerInstanceManager containerInstanceManager;

    readonly IMsiManager     msiManager;
    readonly INetworkManager networkManager;

    readonly IResourceManager resourceManager;
    readonly IStorageManager  storageManager;

    Azure(RestClient restClient, string subscriptionId, string tenantId, IAuthenticated authenticated) {
      resourceManager = ResourceManager.Authenticate(restClient).WithSubscription(subscriptionId);
      storageManager = StorageManager.Authenticate(restClient, subscriptionId);
      networkManager = NetworkManager.Authenticate(restClient, subscriptionId);

      containerInstanceManager = ContainerInstanceManager.Authenticate(restClient, subscriptionId);

      msiManager = MsiManager.Authenticate(restClient, subscriptionId);


      SubscriptionId = subscriptionId;
      this.authenticated = authenticated;
    }

    /// <returns>the currently selected subscription ID this client is authenticated to work with</returns>
    public string SubscriptionId { get; }

    /// <returns>the currently selected subscription this client is authenticated to work with</returns>
    public ISubscription GetCurrentSubscription() => Subscriptions.GetById(SubscriptionId);

    /// <returns>entry point to manage resource groups</returns>
    public IResourceGroups ResourceGroups => resourceManager.ResourceGroups;

    /// <returns>entry point to manage storage accounts</returns>
    public IStorageAccounts StorageAccounts => storageManager.StorageAccounts;

    /// <returns>entry point to manage virtual networks</returns>
    public INetworks Networks => networkManager.Networks;

    /// <returns>entry point to manage network security groups</returns>
    public INetworkSecurityGroups NetworkSecurityGroups => networkManager.NetworkSecurityGroups;

    /// <returns>entry point to manage public IP addresses</returns>
    public IPublicIPAddresses PublicIPAddresses => networkManager.PublicIPAddresses;

    /// <returns>entry point to manage network interfaces</returns>
    public INetworkInterfaces NetworkInterfaces => networkManager.NetworkInterfaces;

    /// <returns>entry point to manage private link services</returns>
    public IPrivateLinkServices PrivateLinkServices => networkManager.PrivateLinkServices;

    /// <returns>entry point to manage Azure firewalls</returns>
    public IAzureFirewalls AzureFirewalls => networkManager.AzureFirewalls;

    /// <returns>entry point to manage route tables</returns>
    public IRouteTables RouteTables => networkManager.RouteTables;

    /// <returns>entry point to manage virtual load balancers</returns>
    public ILoadBalancers LoadBalancers => networkManager.LoadBalancers;

    /// <returns>entry point to manage application gateways</returns>
    public IApplicationGateways ApplicationGateways => networkManager.ApplicationGateways;

    /// <returns>entry point to manage network watchers</returns>
    public INetworkWatchers NetworkWatchers => networkManager.NetworkWatchers;

    /// <returns>entry point to manage Azure Virtual Network Gateways</returns>
    public IVirtualNetworkGateways VirtualNetworkGateways => networkManager.VirtualNetworkGateways;

    /// <returns>entry point to manage Azure Local Network Gateways</returns>
    public ILocalNetworkGateways LocalNetworkGateways => networkManager.LocalNetworkGateways;

    /// <returns>entry point to manage Azure Express Route Circuits</returns>
    public IExpressRouteCircuits ExpressRouteCircuits => networkManager.ExpressRouteCircuits;

    /// <returns>entry point to manage Application Security Gropus</returns>
    public IApplicationSecurityGroups ApplicationSecurityGroups => networkManager.ApplicationSecurityGroups;

    /// <returns>entry point to manage Route Filters</returns>
    public IRouteFilters RouteFilters => networkManager.RouteFilters;

    /// <returns>entry point to manage DDoS protection plans</returns>
    public IDdosProtectionPlans DdosProtectionPlans => networkManager.DdosProtectionPlans;

    /// <returns>entry point to manage deployments</returns>
    public IDeployments Deployments => resourceManager.Deployments;

    /// <returns>entry point to manage policy assignments</returns>
    public IPolicyAssignments PolicyAssignments => resourceManager.PolicyAssignments;

    /// <returns>entry point to manage policy definitions</returns>
    public IPolicyDefinitions PolicyDefinitions => resourceManager.PolicyDefinitions;

    /// <returns>subscriptions that this authenticated client has access to</returns>
    public ISubscriptions Subscriptions => authenticated.Subscriptions;

    public IContainerGroups ContainerGroups => containerInstanceManager.ContainerGroups;

    public IIdentities Identities => msiManager.Identities;

    public IEnumerable<IAzureClient> ManagementClients {
      get {
        var managersList = new List<object>();
        var managerTraversalStack = new Stack<object>();

        managerTraversalStack.Push(this);

        while (managerTraversalStack.Count > 0) {
          var stackedObject = managerTraversalStack.Pop();
          // if not a rollup package
          if (!(stackedObject is IAzure)) {
            managersList.Add(stackedObject);
            var resourceManager = stackedObject.GetType().GetProperty("ResourceManager");
            if (resourceManager != null) managersList.Add(resourceManager.GetValue(stackedObject));
          }

          foreach (var obj in stackedObject
            .GetType().GetFields(BindingFlags.NonPublic | BindingFlags.Instance)
            .Where(f => f.FieldType.GetInterfaces().Contains(typeof(IManagerBase)))
            .Select(f => (IManagerBase) f.GetValue(stackedObject)))
            managerTraversalStack.Push(obj);
        }

        var result = new List<IAzureClient>();
        foreach (var m in managersList) {
          result.AddRange(m.GetType().GetFields(BindingFlags.NonPublic | BindingFlags.Instance)
            .Where(f => f.FieldType.GetInterfaces().Contains(typeof(IAzureClient)))
            .Select(f => (IAzureClient) f.GetValue(m)));

          result.AddRange(m.GetType().GetProperties().Where(n => n.Name.Equals("Inner"))
            .Select(f => (IAzureClient) f.GetValue(m)));
        }
        return result;
      }
    }

    static Authenticated CreateAuthenticated(RestClient restClient, string tenantId) => new Authenticated(restClient, tenantId);

    public static IAuthenticated Authenticate(AzureCredentials azureCredentials) {
      var authenticated = CreateAuthenticated(RestClient.Configure()
        .WithEnvironment(azureCredentials.Environment)
        .WithCredentials(azureCredentials)
        .Build(), azureCredentials.TenantId);
      authenticated.SetDefaultSubscription(azureCredentials.DefaultSubscriptionId);
      return authenticated;
    }

    public static IAuthenticated Authenticate(string authFile) {
      var credentials = SdkContext.AzureCredentialsFactory.FromFile(authFile);
      return Authenticate(credentials);
    }

    public static IAuthenticated Authenticate(RestClient restClient, string tenantId) => CreateAuthenticated(restClient, tenantId);

    public static IConfigurable Configure() => new Configurable();

    public interface IAuthenticated : IAccessManagement {
      string TenantId { get; }

      ITenants Tenants { get; }

      ISubscriptions Subscriptions { get; }

      IAzure WithSubscription(string subscriptionId);

      IAzure WithDefaultSubscription();

      Task<IAzure> WithDefaultSubscriptionAsync();
    }

    protected class Authenticated : IAuthenticated {
      readonly IGraphRbacManager              graphRbacManager;
      readonly ResourceManager.IAuthenticated resourceManagerAuthenticated;
      readonly RestClient                     restClient;
      string                                  defaultSubscription;

      public Authenticated(RestClient restClient, string tenantId) {
        this.restClient = restClient;
        resourceManagerAuthenticated = ResourceManager.Authenticate(this.restClient);
        graphRbacManager = GraphRbacManager.Authenticate(this.restClient, tenantId);
        TenantId = tenantId;
      }

      public string TenantId { get; }

      public ITenants Tenants => resourceManagerAuthenticated.Tenants;

      public ISubscriptions Subscriptions => resourceManagerAuthenticated.Subscriptions;

      public IActiveDirectoryUsers ActiveDirectoryUsers => graphRbacManager.Users;

      public IActiveDirectoryGroups ActiveDirectoryGroups => graphRbacManager.Groups;

      public IActiveDirectoryApplications ActiveDirectoryApplications => graphRbacManager.Applications;

      public IServicePrincipals ServicePrincipals => graphRbacManager.ServicePrincipals;

      public IRoleDefinitions RoleDefinitions => graphRbacManager.RoleDefinitions;

      public IRoleAssignments RoleAssignments => graphRbacManager.RoleAssignments;

      public IAzure WithSubscription(string subscriptionId) => new Azure(restClient, subscriptionId, TenantId, this);

      public IAzure WithDefaultSubscription() {
        if (!string.IsNullOrWhiteSpace(defaultSubscription)) return WithSubscription(defaultSubscription);
        var resourceManager = GetResourceManager();
        var subscriptions = resourceManager.Subscriptions.List();
        var subscription = GetDefaultSubscription(subscriptions);

        return WithSubscription(subscription?.SubscriptionId);
      }

      public async Task<IAzure> WithDefaultSubscriptionAsync() {
        if (!string.IsNullOrWhiteSpace(defaultSubscription)) return WithSubscription(defaultSubscription);
        var resourceManager = GetResourceManager();
        var subscriptions = await resourceManager.Subscriptions.ListAsync().ConfigureAwait(false);
        var subscription = GetDefaultSubscription(subscriptions);

        return WithSubscription(subscription?.SubscriptionId);
      }

      public void SetDefaultSubscription(string subscriptionId) => defaultSubscription = subscriptionId;

      static ISubscription GetDefaultSubscription(IEnumerable<ISubscription> subscriptions) =>
        subscriptions
          .FirstOrDefault(s =>
            StringComparer.OrdinalIgnoreCase.Equals(s.State, "Enabled") ||
            StringComparer.OrdinalIgnoreCase.Equals(s.State, "Warned"));

      ResourceManager.IAuthenticated GetResourceManager() =>
        ResourceManager.Authenticate(
          RestClient.Configure()
            .WithBaseUri(restClient.BaseUri)
            .WithCredentials(restClient.Credentials).Build());
    }

    public interface IConfigurable : IAzureConfigurable<IConfigurable> {
      IAuthenticated Authenticate(AzureCredentials azureCredentials);
    }

    protected class Configurable :
      AzureConfigurable<IConfigurable>,
      IConfigurable {
      IAuthenticated IConfigurable.Authenticate(AzureCredentials credentials) {
        var authenticated = new Authenticated(BuildRestClient(credentials), credentials.TenantId);
        authenticated.SetDefaultSubscription(credentials.DefaultSubscriptionId);
        return authenticated;
      }
    }
  }

  /// <summary>Members of IAzure that are in Beta</summary>
  public interface IAzureBeta : IBeta {
    /// <summary>Entry point to Azure Network Watcher management</summary>
    INetworkWatchers NetworkWatchers { get; }

    /// <summary>Entry point to Azure Virtual Network Gateways management</summary>
    IVirtualNetworkGateways VirtualNetworkGateways { get; }

    /// <summary>Entry point to Azure Local Network Gateways management</summary>
    ILocalNetworkGateways LocalNetworkGateways { get; }

    /// <summary>Entry point to Azure Express Route Circuits management</summary>
    IExpressRouteCircuits ExpressRouteCircuits { get; }

    /// <summary>Entry point to Application Security Groups management</summary>
    IApplicationSecurityGroups ApplicationSecurityGroups { get; }

    /// <summary>Entry point to Route Filters management</summary>
    IRouteFilters RouteFilters { get; }

    /// <summary>Entry point to Azure DDoS protection plans management</summary>
    IDdosProtectionPlans DdosProtectionPlans { get; }

    /// <summary>Entry point to Azure container instance management.</summary>
    IContainerGroups ContainerGroups { get; }

    /// <summary>Entry point to Managed Service Identity (MSI) management.</summary>
    IIdentities Identities { get; }
  }

  public interface IAzure : IAzureBeta {
    /// <summary>Gets all underlying management clients</summary>
    IEnumerable<IAzureClient> ManagementClients { get; }

    string SubscriptionId { get; }

    /// <summary>Entry point to load balancer management.</summary>
    ILoadBalancers LoadBalancers { get; }

    /// <summary>Entry point to application gateway management</summary>
    IApplicationGateways ApplicationGateways { get; }

    ISubscriptions Subscriptions { get; }

    /// <summary>Entry point to resource group management.</summary>
    IResourceGroups ResourceGroups { get; }

    /// <summary>Entry point to storage account management.</summary>
    IStorageAccounts StorageAccounts { get; }

    /// <summary>Entry point to virtual network management.</summary>
    INetworks Networks { get; }

    /// <summary>Entry point to network security group management.</summary>
    INetworkSecurityGroups NetworkSecurityGroups { get; }

    /// <summary>Entry point to public IP address management.</summary>
    IPublicIPAddresses PublicIPAddresses { get; }

    /// <summary>Entry point to network interface management.</summary>
    INetworkInterfaces NetworkInterfaces { get; }

    /// <summary>Entry point to private link service management.</summary>
    IPrivateLinkServices PrivateLinkServices { get; }

    /// <summary>Entry point to Azure firewall management.</summary>
    IAzureFirewalls AzureFirewalls { get; }

    /// <summary>Entry point to route tables management.</summary>
    IRouteTables RouteTables { get; }

    /// <summary>Entry point to Azure Resource Manager template deployment management.</summary>
    IDeployments Deployments { get; }

    /// <summary>Entry point to Azure Resource Manager policy assignment management.</summary>
    IPolicyAssignments PolicyAssignments { get; }

    /// <summary>Entry point to Azure Resource Manager policy definition management.</summary>
    IPolicyDefinitions PolicyDefinitions { get; }

    /// <summary>Returns the subscription the API is currently configured to work with.</summary>
    /// <returns></returns>
    ISubscription GetCurrentSubscription();
  }

  public interface IAccessManagement : IBeta {
    /// <summary>Entry point to Azure Active Directory user management.</summary>
    IActiveDirectoryUsers ActiveDirectoryUsers { get; }

    /// <summary>Entry point to Azure Active Directory group management.</summary>
    IActiveDirectoryGroups ActiveDirectoryGroups { get; }

    /// <summary>Entry point to Azure Active Directory service principal management.</summary>
    IServicePrincipals ServicePrincipals { get; }

    /// <summary>Entry point to Azure Active Directory application management.</summary>
    IActiveDirectoryApplications ActiveDirectoryApplications { get; }

    /// <summary>Entry point to Azure Active Directory role definition management.</summary>
    IRoleDefinitions RoleDefinitions { get; }

    /// <summary>Entry point to Azure Active Directory role assignemnt management.</summary>
    IRoleAssignments RoleAssignments { get; }
  }
}