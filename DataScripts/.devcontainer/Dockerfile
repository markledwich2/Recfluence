#-------------------------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See https://go.microsoft.com/fwlink/?linkid=2090316 for license information.
#-------------------------------------------------------------------------------------------------------------

# Update the VARIANT arg in devcontainer.json to pick a Python version: 3, 3.8, 3.7, 3.6 
# To fully customize the contents of this image, use the following Dockerfile instead:
# https://github.com/microsoft/vscode-dev-containers/tree/v0.112.0/containers/python-3/.devcontainer/base.Dockerfile
ARG VARIANT="3.9"
FROM mcr.microsoft.com/vscode/devcontainers/python:0-${VARIANT}

RUN pip install --upgrade pip && pip install setuptools wheel spacy
RUN python -m spacy download en_core_web_sm

# turns out we don't need this?
# install snowflake requirements (as told to https://docs.snowflake.com/en/user-guide/python-connector-install.html#python-package-installer-and-setup-tools)
#RUN pip install -r https://raw.githubusercontent.com/snowflakedb/snowflake-connector-python/master/tested_requirements/requirements_38.reqs

#ENV DEBIAN_FRONTEND=noninteractive
#RUN apt-get update && apt-get -y install --no-install-recommends 
# Clean up
#RUN apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/*
#ENV DEBIAN_FRONTEND=dialog
