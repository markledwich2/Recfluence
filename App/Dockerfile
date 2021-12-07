ARG SEMVER
ARG ASSEMBLY_SEMVER

FROM mcr.microsoft.com/dotnet/sdk:6.0-alpine AS build-env

COPY YtCli YtCli/
COPY YtReader YtReader/
COPY SysExtensions SysExtensions/
COPY Mutuo.Etl Mutuo.Etl/
COPY Mutuo.Tools Mutuo.Tools/

ARG SEMVER
ARG ASSEMBLY_SEMVER
RUN echo SemVer={$SEMVER} AssemblySemVer={$ASSEMBLY_SEMVER}

WORKDIR /YtCli
RUN dotnet publish -c Release -o publish /p:Version=${ASSEMBLY_SEMVER} /p:InformationalVersion=${SEMVER}

# build runtime image
FROM mcr.microsoft.com/dotnet/runtime:6.0-alpine
WORKDIR /app
COPY --from=build-env YtCli/publish  ./

# humanizer and possibly other libraries rely on having a culture
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=false
RUN apk add icu-libs
# it is more flexible to work without the entrypoint. And azure doesn't allow args when entrypoint is used.
#ENTRYPOINT ["dotnet", "ytnetworks.dll"]
