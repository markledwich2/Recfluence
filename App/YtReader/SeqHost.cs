using System;
using System.Threading.Tasks;
using Humanizer;
using Mutuo.Etl.AzureManagement;
using Mutuo.Etl.Pipe;
using Serilog;
using Serilog.Events;
using SysExtensions.Threading;

namespace YtReader {
  public class SeqHost {
    readonly SeqCfg       Cfg;
    readonly PipeAzureCfg AzureCfg;

    public SeqHost(SeqCfg cfg, PipeAzureCfg azureCfg) {
      Cfg = cfg;
      AzureCfg = azureCfg;
    }

    /// <summary>Kick of a restart on seq if needed (doesn't wait for it)</summary>
    public async Task StartSeqIfNeeded() {
      var log = new LoggerConfiguration()
        .WriteTo.Console(LogEventLevel.Information).CreateLogger();
      if (Cfg.SeqUrl.IsLoopback)
        return;
      try {
        var azure = AzureCfg.GetAzure();
        var seqGroup = await azure.SeqGroup(Cfg, AzureCfg);
        if (seqGroup.State() != ContainerState.Running) {
          await azure.ContainerGroups.StartAsync(seqGroup.ResourceGroupName, seqGroup.Name);
          var seqStart = await seqGroup.WaitForState(ContainerState.Running).WithTimeout(30.Seconds());
          log.Information(seqStart.Success ? "{SeqUrl} started" : "{SeqUrl} launched but not started yet", Cfg.SeqUrl);
        }
        else {
          log.Information("Seq connected on {SeqUrl}", Cfg.SeqUrl);
        }
      }
      catch (Exception ex) {
        log.Error(ex, "Error starting seq: {Error}", ex.Message);
      }
    }
  }
}