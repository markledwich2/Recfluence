
# FYI, this should be build with the current dir as root of the Repo
# This makes it easier to use .git (required for versioning)


FROM mcr.microsoft.com/dotnet/core/sdk:3.1-alpine AS build-env
WORKDIR /YtNetworks

RUN apk update
RUN apk add git libgit2
RUN dotnet tool restore


# try get get a cache of the nuget restore
# https://andrewlock.net/optimising-asp-net-core-apps-in-docker-avoiding-manually-copying-csproj-files/
WORKDIR /YtNetworks/App
COPY App/YtNetworks.sln YtNetworks.sln
# COPY App/YtCli/YtCli.csproj YtCli/YtCli.csproj
# COPY App/YtReader/YtReader.csproj YtReader/YtReader.csproj
# COPY App/SysExtensions/SysExtensions.csproj SysExtensions/SysExtensions.csproj
# COPY App/Mutuo.Etl/Mutuo.Etl.csproj Mutuo.Etl/Mutuo.Etl.csproj
# COPY App/Mutuo.Tools/Mutuo.Tools.csproj Mutuo.Tools/Mutuo.Tools.csproj
# RUN dotnet restore SysExtensions
# RUN dotnet restore Mutuo.Tools
# RUN dotnet restore Mutuo.Etl
# RUN dotnet restore YtCli
# RUN dotnet restore YtReader

# copy git to be able to use GitVersion
WORKDIR /YtNetworks
COPY .git .git

WORKDIR /YtNetworks/App
COPY App/YtCli YtCli/
COPY App/YtReader YtReader/
COPY App/SysExtensions SysExtensions/
COPY App/Mutuo.Etl Mutuo.Etl/
COPY App/Mutuo.Tools Mutuo.Tools/

WORKDIR /YtNetworks/App/Mutuo.Tools
RUN dotnet run gitversion 

# publish YtCli
WORKDIR /YtNetworks/App/YtCli
RUN dotnet publish -r linux-musl-x64 --self-contained -c Release -o publish /p:PublishSingleFile=true /p:PublishTrimmed=true

# build runtime image
FROM mcr.microsoft.com/dotnet/core/runtime-deps:3.1-alpine
WORKDIR /app
COPY --from=build-env YtNetworks/App/YtCli/publish  ./

# it is more flexible to work without the entrypoint. And azure doesn't allow args when entrypoint is used.
#ENTRYPOINT ["dotnet", "ytnetworks.dll"]