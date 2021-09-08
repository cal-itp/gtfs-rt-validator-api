FROM openjdk:11

ENV GTFS_VALIDATOR_JAR=/gtfs-realtime-validator.jar
ENV GTFS_VALIDATOR_VERSION=v1.0.0

WORKDIR /

RUN wget \
    https://s3.amazonaws.com/gtfs-rt-validator/travis_builds/gtfs-realtime-validator-lib/1.0.0-SNAPSHOT/gtfs-realtime-validator-lib-1.0.0-SNAPSHOT.jar \
    -O ${GTFS_VALIDATOR_JAR}

WORKDIR /app

