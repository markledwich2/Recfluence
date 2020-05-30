# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3-slim

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get -y install --no-install-recommends chromium-driver \
    #
    # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*
ENV DEBIAN_FRONTEND=dialog

# upgrade pip
RUN pip install --upgrade pip

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

# Install pip requirements
ADD requirements.txt .
RUN python -m pip install -r requirements.txt

WORKDIR /app
ADD . /app

# During debugging, this entry point will be overridden. For more information, refer to https://aka.ms/vscode-docker-python-debug
CMD ["python", "app.py"]