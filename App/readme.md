# Recfluence App

.NET solution to perform all back-end recfluence tasks.

# Development Setup (vscode)

In VSCode launch the dev container. Command: `Remote Containers - Reopen in container`


In `/YtCli` create a `local.rootcfg.json` file and populate appropreate &lt;values&gt;
```json
{
  "env": "dev",
  "appStoreCs": "<a connection string to the blob container containing futher settings. This will be given to you>",
  "branchEnv": "<a short suffix to use with a variety of cloud resource to make you dev environment unique. e.g.>"
}
```

Create your development environment (blob containers and warehouse)

```bash
cd /YtCli
dotnet run -- create-env
```


## YtCli
Command line tool for performing all back-end operations. 

```bash
dotnet run #see the list of possible commands
dotnet run -- update -help #see help for the update command
```

### Examples:
dotnet run --
- `update -a Collect -c  UCWVMHyIWEvAWv3Lc1C5icVA -p channel|extra`

refreshes channel and video data for a specific channel

TODO: more examples


## YtFunctions
An Azure function for triggering daily updates, and as the clinet API for websites.

TODO: how to debug

## Warehouse