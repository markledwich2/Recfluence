ARG SEMVER
ARG ASSEMBLY_SEMVER

# generic runtime image with chrome
FROM mcr.microsoft.com/dotnet/core/runtime:latest as run

# Install latest chrome dev package
# Note: this installs the necessary libs to make the bundled version of Chromium that Puppeteer
# installs, work.
RUN apt-get update && apt-get -f install && apt-get -y install wget gnupg2 apt-utils
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list' \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
      --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

ENV PUPPETEER_EXECUTABLE_PATH "/usr/bin/google-chrome-stable"

#build the YtCli project
FROM mcr.microsoft.com/dotnet/core/sdk:3.1 AS build-env

# restore all csproj frst and docker wil cache this layer nicely
COPY */*.csproj ./
#RUN for projectName in *.csproj; do dotnet restore $projectName; done

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

FROM run
WORKDIR /app
COPY --from=build-env YtCli/publish  ./

#CMD ["dotnet", "recfluence.dll"] 