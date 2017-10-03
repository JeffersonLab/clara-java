FROM openjdk:8-jdk-slim AS build

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install --assume-yes apt-utils

# Useful packages
RUN apt-get update && apt-get install -y \
        ca-certificates \
        curl \
        procps \
        wget \
    && rm -rf /var/lib/apt/lists/*

ENV CLARA_HOME="/usr/local/clara"
ENV PATH="${CLARA_HOME}/bin:${PATH}"

RUN mkdir -p /usr/local/src/clara-java
WORKDIR /usr/local/src/clara-java

COPY gradlew .
COPY gradle gradle
RUN ./gradlew -v

COPY build.gradle .
RUN ./gradlew build check

COPY . .
RUN ./gradlew build -x check && ./gradlew deploy && rm -rf build

WORKDIR /usr/local/src
VOLUME ["${CLARA_HOME}/data/input", "${CLARA_HOME}/data/output", "${CLARA_HOME}/log"]

EXPOSE 7771-7775


FROM openjdk:8-jre-slim

# Useful packages
RUN apt-get update && apt-get install -y \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV CLARA_HOME="/usr/local/clara"
ENV PATH="${CLARA_HOME}/bin:${PATH}"

COPY --from=build ${CLARA_HOME} ${CLARA_HOME}

WORKDIR /root
VOLUME ["${CLARA_HOME}/data/input", "${CLARA_HOME}/data/output", "${CLARA_HOME}/log"]

EXPOSE 7771-7775
