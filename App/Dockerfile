ARG SEMVER
ARG ASSEMBLY_SEMVER

FROM mcr.microsoft.com/dotnet/core/sdk:3.1-alpine AS build-env

# restore all csproj frst and docker wil cache thislayer nicely
COPY */*.csproj ./
RUN for projectName in *.csproj; do dotnet restore $projectName; done

COPY YtCli YtCli/
COPY YtReader YtReader/
COPY SysExtensions SysExtensions/
COPY Mutuo.Etl Mutuo.Etl/
COPY Mutuo.Tools Mutuo.Tools/

ARG SEMVER
ARG ASSEMBLY_SEMVER
RUN echo SemVer={$SEMVER} AssemblySemVer={$ASSEMBLY_SEMVER}

# publish YtCli
# https://docs.microsoft.com/en-us/dotnet/standard/library-guidance/versioning
WORKDIR /YtCli
RUN dotnet publish -c Release -o publish /p:Version=${ASSEMBLY_SEMVER} /p:InformationalVersion=${SEMVER}
#RUN dotnet publish -r linux-musl-x64 --self-contained -c Release -o publish /p:PublishSingleFile=true /p:PublishTrimmed=true

# build runtime image
FROM mcr.microsoft.com/dotnet/core/runtime:3.1-alpine
WORKDIR /app
COPY --from=build-env YtCli/publish  ./

# it is more flexible to work without the entrypoint. And azure doesn't allow args when entrypoint is used.
#ENTRYPOINT ["dotnet", "ytnetworks.dll"]