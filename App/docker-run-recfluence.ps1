$rootCfg = Get-content ./YtCli/local.rootcfg.json | ConvertFrom-Json
$cs = $rootCfg.appStoreCs
docker run -e env=dev -e appStoreCs="$cs" ytnetworks.azurecr.io/recfluence:latest ./recfluence test-chrome-scraper -v fJoAPMWk4zc