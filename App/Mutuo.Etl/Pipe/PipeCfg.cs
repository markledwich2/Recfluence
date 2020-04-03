﻿using System;
using SysExtensions.Security;
using SysExtensions.Text;

namespace Mutuo.Etl.Pipe {
  public enum PipeRunLocation {
    Container,
    LocalContainer,
    LocalThread
  }

  public class PipeAppCfg {
    public PipeAppStorageCfg Store         { get; set; }
    public PipeAzureCfg      Azure         { get; set; }
    public ContainerCfg      Container     { get; set; }
    public PipeRunLocation   Location      { get; set; }
    public int               LocalParallel { get; set; } = 2;
  }

  public class PipeAzureCfg {
    public string              SubscriptionId   { get; set; }
    public ServicePrincipalCfg ServicePrincipal { get; set; }
    public string              ResourceGroup    { get; set; }
  }

  public class ServicePrincipalCfg {
    public string ClientId  { get; set; }
    public string Secret    { get; set; }
    public string TennantId { get; set; }
  }

  public class ContainerCfg {
    public string     Registry      { get; set; }
    public string     Name          { get; set; }
    public string     ImageName     { get; set; }
    public string     Tag           { get; set; }
    public int        Cores         { get; set; }
    public double     Mem           { get; set; }
    public NameSecret RegistryCreds { get; set; }
    public string     Region        { get; set; } = Microsoft.Azure.Management.ResourceManager.Fluent.Core.Region.USWest2.Name;
    public string     Exe           { get; set; }
  }

  public class PipeAppStorageCfg {
    public string     Cs   { get; set; }
    public StringPath Path { get; set; }
  }

}