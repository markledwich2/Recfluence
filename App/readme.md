# Recfluence App

.NET solution to perform all back-end recfluence tasks.


## Dev Configuration


In `/YtCli` create a `local.rootcfg.json` file. This includes the bare minimum information to give access to the variety of cloud services used by recfluence. The &lt;values&gt;should be provided to you. It would be possible to create the required resources (eg.. snowflake, azure blob storage etc..) but that is not currently automated/documented.
```json
{
  "env": "dev",
  "appStoreCs": "<a connection string to the blob container containing futher settings. This will be given to you>",
  "branchEnv": "<a short suffix to use with a variety of cloud resource to make you dev environment unique. e.g.>"
}
```

## Casual Development Setup (vscode)

In VSCode launch the dev container. Command: `Remote Containers - Reopen in container`

To create your development environment (blob containers and warehouse).

```bash
cd /YtCli
dotnet run -- create-env
```
*TODO: this should provide information about how to connect via other tooling to those things.*


## Dev Setup (rider)
Best editing experience.

- Install [Rider](https://www.jetbrains.com/rider/)
- Install [.NET 5 SDK](https://dotnet.microsoft.com/download/dotnet/5.0)
- `dotnet tool restore` (installs GitVersion)
- Open **Recfluence.sln** in rider
A variety of pre-configured debugging

## YtCli
Command line tool for performing all back-end operations. 

```bash
dotnet run #see the list of possible commands
dotnet run -- update -help #see help for the update command
```

### Examples:
dotnet run --
- `update -a Collect -c UCWVMHyIWEvAWv3Lc1C5icVA -p channel|extra`

refreshes channel and video data for a specific channel

TODO: more examples


## YtFunctions
An Azure function for triggering daily updates, and as the clinet API for websites.

TODO: how to debug

## Warehouse