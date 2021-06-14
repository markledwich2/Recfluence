# Recfluence App

.NET solution to perform all back-end recfluence tasks.

## Dev Setup 

### Dev Tooling
Rider is the recommended way to develop in this solution
- Install [Rider](https://www.jetbrains.com/rider/)
- Install [.NET 5 SDK](https://dotnet.microsoft.com/download/dotnet/5.0)
- `dotnet tool restore` (installs GitVersion)
- Open **Recfluence.sln** in rider
A variety of pre-configured debugging


### Minimal Configuration
In `/YtCli` create a `local.rootcfg.json` file. This includes the bare minimum information to give access to the variety of cloud services used by recfluence. The &lt;values&gt;should be provided to you. 
```json
{
  "env": "dev",
  "appStoreCs": "<a connection string to the blob container containing further settings. This will be given to you>",
  "branchEnv": "<a short suffix to use with a variety of cloud resource to make your dev environment unique>"
}
```

### Environment setup and configuration
The cloud services used have configuration that has been created by hand (i.e. snowflake instance, blob storage, elastic search). However, a "branch environment" can be created for development and testing which has a separate copy of staging, warehouse, results and search.

The recommended development setup is to by default have a branchEnv configured for you personally, and use the production environment case-by case with the branchEnv environment variable.

To create a new branch environment:
1. in `/YtCli/local.rootcfg.json` set "branchEnv" to something small and alpha-number (e.g. test)
2. creates an empty new storage container and clone of the current prod warehouse (reconfigured to point to this new storage)
```bash
dot run -- branch-env -m CloneDb
```

To test, connect to snowflake with DataGrip and switch to your new warehouse YT_TEST. If you are developing a new feature that is used by front-ends, they will have a separate configuration to use a branch environment.

NOTE:
- Re-running this will override you test environment with the latest from production. 
- use `-m Clone` if you also want a clone of blob storage from production. This will take some time and is not usually needed.

## YtCli (recfluence.exe command line tool)
Command line tool for performing all back-end operations. 

```bash
dotnet run
```
![image](https://user-images.githubusercontent.com/17095341/121965628-a7e9fd00-cdb0-11eb-95fc-503fc0874398.png)


The commonly-used commands are **update** and **collect-list** which have some common use cases bellow. For help on each option  documented below, for the rest use the `--help` option.

### update
Performs a regular daily update i.e. data scraping > warehouse update > results/search/index. The long list of options allow you to narrow down the update just to what you need for debugging/development.

```bash
dotnet run -- update --help
```
![image](https://user-images.githubusercontent.com/17095341/121965000-bab00200-cdaf-11eb-8773-74192e53a944.png)


### default update
```bash
# run a default update on this machine
dotnet run -- update

# launch a container to run the default update
dotnet run -- update -z
```
Performs the default update (same as what is triggered daily). All actions are run in dependency order (See [YtUpdater.cs](YtReader/YtUpdater.cs) for up-to-date actions and dependencies).
- **Collect**: scrapes data from YouTube
- **BitchuteCollect**: scrapes BitChute
- **RumbleCollect**: scapes rumble
- **Stage**: optimizes josnl in blob storage that hasn't yet been loaded (combining them into ~200MB files), then loads them onto `*_stage` tables in snowflake.
- **Dataform**: runs the `standard` tag action in the dataform project
- **Search**: updates the elastic search data with new or updated channels, videos and captions
- **Result**: saves files to blob storage that are used by front-end's or 3rd parties
- **Index**: saves indexed files blob storage that are used by front-end's or 3rd parties
- **DataScripts**: executes all python data scripts (currently just named entity recognition).

<br /><br />
### scrape subset of channels/parts
```bash
# scrape only the channel info for one channel
dotnet run -- update -a Collect -c UCWVMHyIWEvAWv3Lc1C5icVA -p channel

 # scrape video details and missing captions for videos in two channels
dotnet run -- update -a Collect -c UCWVMHyIWEvAWv3Lc1C5icVA|UCJm5yR1KFcysl_0I3x-iReg -p channel-video|extra -e extra|caption
```
Performs a YouTube collect for a specific channel (`-c UCWVMHyIWEvAWv3Lc1C5icVA`). It will scrape the channel stats and video extras (skipping transcriptions, comments and recommendations `-p channel|extra`). Only collection is performed, it will remain in blob storage un-optimized and not loaded into the warehouse (`a Collect`)
<br /><br />

### update index/result
Often when developing in a test environment, you will make data updates directly in DataForm. To see the new data in front-end tools, you will need to update the Index or Result.
```bash
# Update all index resuts tagged narrative2
dotnet run -- update -a Index -t narrative2 

# Update political channel list (used by transparency tube)
dotnet run -- update -a Result -r ttube_channels
```
 (`-t narrative2`). Index results are compress json optimized for a specific front-end visualization. They are usually partitioned by a time range or filter, and aggregated to the granularity used by a viz/list.



## YtFunctions
An Azure function for triggering daily updates, and as the client API for websites.

TODO: how to debug
