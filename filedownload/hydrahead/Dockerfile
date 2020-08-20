FROM ubuntu

MAINTAINER Alexander Lachmann <alexander.lachmann@mssm.edu>

# Python installs
RUN apt-get update && apt-get install -y \
    python \
    python-dev \
    python-pip \
    python-setuptools \
    python-h5py

RUN cd /usr/local/src           ; \
    pip install --upgrade pip   ; \
    pip install tornado         ; \
    pip install h5py            ; \
    pip install numpy           ; \
    pip install pandas          ; \
    pip install urllib          ; \
    pip install numpy

WORKDIR /usr/local/src

RUN mkdir -p /app/tornado/data
COPY . /app/tornado

EXPOSE 5000

WORKDIR /app/tornado

RUN chmod -R 777 /app/tornado
ENTRYPOINT ./entrypoint.sh
