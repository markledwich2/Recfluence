FROM python:3.9-slim

RUN pip install --upgrade pip

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

# Install pip requirements
ADD requirements.txt .
RUN pip install -r requirements.txt

WORKDIR /app
ADD . /app

RUN python -m spacy download en_core_web_sm

# During debugging, this entry point will be overridden. For more information, refer to https://aka.ms/vscode-docker-python-debug
CMD ["python", "app.py"]