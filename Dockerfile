FROM python:3

# Base image for FMI S3 uploader

RUN pip install --upgrade pip requests python-dateutil boto3 filechunkio

# Bundle app source
ADD . /src
WORKDIR /src

RUN mkdir /gribs
