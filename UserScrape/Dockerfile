# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.8-alpine3.10

# update apk repo
RUN echo "https://dl-4.alpinelinux.org/alpine/v3.10/main" >> /etc/apk/repositories && \
    echo "https://dl-4.alpinelinux.org/alpine/v3.10/community" >> /etc/apk/repositories

# install chromedriver
RUN apk update
RUN apk add chromium chromium-chromedriver build-base libffi-dev openssl-dev

# upgrade pip
RUN pip install --upgrade pip

#
# install local proxy to work around problem with authentication by headless chromium
#

#depends on glibc which alpine doesn't like. this is a workaround i found https://github.com/sgerrand/alpine-pkg-glibc 
RUN wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub
RUN wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.31-r0/glibc-2.31-r0.apk
RUN apk add glibc-2.31-r0.apk

RUN wget -O /usr/bin/crawlera-headless-proxy https://github.com/scrapinghub/crawlera-headless-proxy/releases/download/1.2.1/crawlera-headless-proxy-linux-amd64
RUN chmod +x /usr/bin/crawlera-headless-proxy

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
CMD ["python", "yt_scraper.py"]