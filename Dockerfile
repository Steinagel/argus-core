FROM python:3.7

WORKDIR /usr/src/app
ENV PYTHONPATH /usr/src/app
ENV PYTHONUNBUFFERED 1

RUN apt-get update -y && apt-get upgrade -y

# RUN apt-get install -y pkg-config apt-utils debconf-utils build-essential python3-dev default-libmysqlclient-dev
# RUN apt-get install -y librabbitmq-dev libmemcached-dev libxml2-dev libxmlsec1-dev libxmlsec1-openssl
RUN apt-get install -y librabbitmq-dev libmemcached-dev libxml2-dev libxmlsec1-dev libxmlsec1-openssl

RUN apt autoclean
RUN apt clean

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt
# RUN python -m nltk.downloader stopwords wordnet wordnet_ic punkt sentiwordnet
RUN pip install watchdog==0.9.0

COPY README.md README.md

COPY app app
COPY setup.py setup.py

RUN python setup.py install

# RUN pip install -r requirements.txt

# COPY . .
# CMD gunicorn -w 6 -b 0.0.0.0:8000 app.__main__:app --timeout=1200 --log-level=debug
