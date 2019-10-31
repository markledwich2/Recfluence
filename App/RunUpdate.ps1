$cs = Get-ChildItem Env:YtNetworks_AzureStorageCs
$csParam = [string]::Format("{0}={1}", $cs.Name, $cs.Value)
docker run --env $csParam --env YtNetworks_Env=Prod ytnetworks.azurecr.io/ytnetworks dotnet ytnetworks.dll update