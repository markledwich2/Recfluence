using System.ComponentModel.DataAnnotations;
using SysExtensions.Security;
using SysExtensions.Text;

namespace Mutuo.Etl.Pipe {
  public enum PipeRunLocation {
    Local,
    Container,
    LocalContainer
  }

  public class PipeAppCfg {
    [Required] public PipeAppStorageCfg Store         { get; set; } = new();
    [Required] public PipeAzureCfg      Azure         { get; set; } = new();
    [Required] public PipeRunLocation   Location      { get; set; } = PipeRunLocation.Local;
    [Required] public int               LocalParallel { get; set; } = 2;
    [Required] public PipeRunCfg        Default       { get; set; } = new();
    public            NamedPipeRunCfg[] Pipes         { get; set; } = { };
  }

  public class PipeAzureCfg {
    public            string              SubscriptionId   { get; set; }
    [Required] public ServicePrincipalCfg ServicePrincipal { get; set; } = new();
    public            string              ResourceGroup    { get; set; }

    /// <summary>The default parallelism when working with azure</summary>
    [Required]
    public int Parallel { get; set; } = 8;
  }

  public class ServicePrincipalCfg {
    [Required] public string ClientId  { get; set; }
    [Required] public string Secret    { get; set; }
    [Required] public string TennantId { get; set; }
  }

  public record ContainerCfg {
    public string Registry  { get; set; }
    public string ImageName { get; set; }
    //public string     BaseTag           { get; set; }
    public int        Cores         { get; set; }
    public double     Mem           { get; set; }
    public NameSecret RegistryCreds { get; set; }
    public string     Region        { get; set; } = Microsoft.Azure.Management.ResourceManager.Fluent.Core.Region.USWest2.Name;
    public string     Exe           { get; set; }
  }

  public class PipeAppStorageCfg {
    [Required] public string     Cs   { get; set; } = "UseDevelopmentStorage=true";
    [Required] public StringPath Path { get; set; } = "pipe";

    /// <summary>The default parallelism when working with pipe storage</summary>
    [Required]
    public int Parallel { get; set; } = 8;
  }

  public class NamedPipeRunCfg : PipeRunCfg {
    [Required] public string PipeName { get; set; }
  }

  public class PipeRunCfg {
    /// <summary>The min number of work itms for a batch</summary>
    public int MinWorkItems { get; set; } = 100;

    /// <summary>The max number of parellel work runners to be created</summary>
    public int MaxParallel { get; set; } = 4;

    public bool ReturnOnStart { get; set; }

    [Required] public ContainerCfg Container { get; set; } = new();
  }
}