$rootCfg = Get-content ./YtCli/local.rootcfg.json | ConvertFrom-Json
$cs = $rootCfg.appStoreCs
docker run -e env=dev -e appStoreCs="$cs" ytnetworks.azurecr.io/ytnetworks:0.5.0-fullupdate ./ytnetworks update -a Search