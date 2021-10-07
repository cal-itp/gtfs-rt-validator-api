FROM openjdk:11

ENV GTFS_VALIDATOR_JAR=/gtfs-realtime-validator.jar
ENV GTFS_VALIDATOR_VERSION=v1.0.0

WORKDIR /

RUN wget \
    https://s3.amazonaws.com/gtfs-rt-validator/travis_builds/gtfs-realtime-validator-lib/1.0.0-SNAPSHOT/gtfs-realtime-validator-lib-1.0.0-SNAPSHOT.jar \
    -O ${GTFS_VALIDATOR_JAR}

# Install python
RUN apt-get update -y \
    && apt-get install -y python3 python3-pip \
    && python3 -m pip install argh==0.26.2 gcsfs==0.8.0

# Install package
WORKDIR /application

ADD . ./

RUN python3 -m pip install .
