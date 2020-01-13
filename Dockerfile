FROM python:3.7-alpine

COPY . /app

RUN apk update \
    && \
    apk upgrade \
    && \
    apk add \
      bash \
      vim \
      build-base \
      jpeg-dev \
      libffi-dev \
      openssl-dev \
      zlib-dev  \
    && \
    pip install --upgrade pip \
    && \
    pip install -r /app/requirements.txt \
    && \
    apk del \
      jpeg-dev \
      libffi-dev \
      openssl-dev \
      zlib-dev  

WORKDIR /app
CMD /usr/local/bin/python run.py

