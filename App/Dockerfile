
# FYI, this should be build with the current dir as root of the Repo
# This makes it easier to use .git (required for versioning)


FROM mcr.microsoft.com/dotnet/core/sdk:3.1-alpine AS build-env
WORKDIR /YtNetworks

RUN apk update
RUN apk add git libgit2
RUN dotnet new tool-manifest
RUN dotnet tool install GitVersion.Tool

# try get get a cache of the nuget restore
# https://andrewlock.net/optimising-asp-net-core-apps-in-docker-avoiding-manually-copying-csproj-files/
WORKDIR /YtNetworks/App
COPY App/YtNetworks.sln YtNetworks.sln
COPY App/YtCli/YtCli.csproj YtCli/YtCli.csproj
COPY App/YtReader/YtReader.csproj YtReader/YtReader.csproj
COPY App/SysExtensions/SysExtensions.csproj SysExtensions/SysExtensions.csproj
COPY App/Tests/Tests.csproj Tests/Tests.csproj
COPY App/Mutuo.Etl/Mutuo.Etl.csproj Mutuo.Etl/Mutuo.Etl.csproj
COPY App/Mutuo.Tools/Mutuo.Tools.csproj Mutuo.Tools/Mutuo.Tools.csproj
COPY App/YtFunctions/YtFunctions.csproj YtFunctions/YtFunctions.csproj
RUN dotnet restore

# copy everything in App and build
WORKDIR /YtNetworks
COPY .git .git
COPY App App
RUN ls -la


WORKDIR /YtNetworks/App
RUN ls -la

WORKDIR /YtNetworks/App/Mutuo.Tools
RUN dotnet run -- gitversion

# publish YtCli
WORKDIR /YtNetworks/App/YtCli
RUN dotnet publish -c Release -o publish

# build runtime image
FROM mcr.microsoft.com/dotnet/core/runtime:3.1-alpine
WORKDIR /app
COPY --from=build-env YtNetworks/App/YtCli/publish  ./

ENTRYPOINT ["dotnet", "ytnetworks.dll"]