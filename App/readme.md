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


## Dev Setup (JetBrains Rider)
Rider is the recommended way to develop in this solution. It contains automatic code style configuration and easy access to common commands.

- Install [Rider](https://www.jetbrains.com/rider/)
- Install [.NET 5 SDK](https://dotnet.microsoft.com/download/dotnet/5.0)
- `dotnet tool restore` (installs GitVersion)
- Open **Recfluence.sln** in rider
A variety of pre-configured debugging

## YtCli
Command line tool for performing all back-end operations. 

```bash
dotnet run #see the list of possible commands
dotnet run -- update --help #see help for the update command
```

![image](https://user-images.githubusercontent.com/17095341/121848256-22713900-cd2d-11eb-9c4b-0d307e599a23.png)

The commonly-used commands are **update** and **collect-list** which are documented below, for the rest use the `--help` option.

### > recfluence update
![image](https://user-images.githubusercontent.com/17095341/121848927-1174f780-cd2e-11eb-9b46-4b97bd38a0a5.png)

### Examples:
```bash
dotnet run -- update -z
```
Launches a container (`-z`) to form the default update (same as what is triggered daily) . It will peform all actions and run in dependency order
- **Collect**: scrapes data from YouTube
- **BitchuteCollect**: scrapes bichute
- **RumbleCollect**: scapes rumble
- **Stage**: optimizes josnl in blob storage that hasn't yet been loaded (combining them into ~200MB files), then loads them onto `*_stage` tables in snowflake.
- **Dataform**: runs the `standard` tag action in the dataform project
- **Search**: updates the elastic search data with new or updated channels, videos and captions
- **Result**: saves files to blob storage that are used by front-end's or 3rd parties
- **Index**: saves indexed files blob storage that are used by front-end's or 3rd parties
- **DataScripts**: executes all python data scripts (currently just named entity recognition).

<br /><br />
```bash
dotnet run -- update -a Collect -c UCWVMHyIWEvAWv3Lc1C5icVA -p channel|extra
```
Perofrm's a YouTube collect for a specific channel (`-c UCWVMHyIWEvAWv3Lc1C5icVA`). It will scrape the channel stats and video extras (skipping transcriptions, comments and recommendations `-p channel|extra`). Only collection is performed, it will remain in blob storage un-optimized and not loaded into the warehouse (`a Collect`)
<br /><br />

```bash
dotnet run -- update -a Index -t narrative2
```
Updates all index resuts taggs narrative2 (`-t narrative2`). Index results are compress json optimized for a specific front-end vizualisation. They are usually partitioned by a time range or filter, and aggregated to the granularity used by a viz/list.



## YtFunctions
An Azure function for triggering daily updates, and as the clinet API for websites.

TODO: how to debug

## Warehouse
