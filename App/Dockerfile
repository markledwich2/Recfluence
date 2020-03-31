
# FYI, this should be build with the current dir as root of the Repo
# This makes it easier to use .git (required for versioning)


FROM mcr.microsoft.com/dotnet/core/sdk:3.1-alpine AS build-env
WORKDIR /YtNetworks

# copy everything in App and build
COPY .git .git
COPY App App
RUN ls -la

WORKDIR /YtNetworks/App
RUN ls -la

WORKDIR /YtNetworks/App/Mutuo.Tools
RUN apk update
RUN apk add git
RUN apk add libgit2
RUN dotnet new tool-manifest
RUN dotnet tool install GitVersion.Tool
RUN dotnet run -- gitversion

# publish YtCli
WORKDIR /YtNetworks/App/YtCli
RUN dotnet publish -c Release -o publish

# build runtime image
FROM mcr.microsoft.com/dotnet/core/runtime:3.1-alpine
WORKDIR /app
COPY --from=build-env YtNetworks/App/YtCli/publish  ./

#ENTRYPOINT ["dotnet", "ytnetworks.dll"] 