#FROM microsoft/dotnet:2.1-runtime-deps-alpine
#RUN wget -P tools/ https://ytnetworks.blob.core.windows.net/tools/mledwich-yt.1.0.0.nupkg
#COPY publish/ tools/
#RUN dotnet tool install --global --add-source ./tools/ ytnetworks
#ENV PATH "$PATH:/root/.dotnet/tools"

FROM microsoft/dotnet:2.1-sdk AS build-env
WORKDIR /app

# copy everything else and build
COPY SysExtensions SysExtensions
COPY YtCli YtCli
COPY YtReader YtReader
WORKDIR /app/YtCli
#RUN dotnet restore
RUN dotnet publish -c Release -o publish

# build runtime image
FROM microsoft/dotnet:2.1-runtime-alpine
WORKDIR /app
COPY --from=build-env /app/YtCli/publish  ./
#ENTRYPOINT ["dotnet", "ytnetworks.dll"] 