
FROM mcr.microsoft.com/dotnet/core/sdk:2.2 AS build-env
WORKDIR /app

# copy everything else and build
COPY SysExtensions SysExtensions
COPY YtCli YtCli
COPY YtReader YtReader
WORKDIR /app/YtCli
#RUN dotnet restore
RUN dotnet publish -c Release -o publish

# build runtime image
FROM mcr.microsoft.com/dotnet/core/runtime:2.2-alpine
WORKDIR /app
COPY --from=build-env /app/YtCli/publish  ./
#ENTRYPOINT ["dotnet", "ytnetworks.dll"] 