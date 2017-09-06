FROM openjdk:8-jdk-slim AS build

ENV CLARA_HOME="/usr/local/clara"
ENV PATH="${CLARA_HOME}/bin:${PATH}"

RUN mkdir -p /root/clara-java
WORKDIR /root/clara-java

COPY gradlew .
COPY gradle gradle
RUN ./gradlew -v

COPY build.gradle .
RUN ./gradlew build check

COPY . .
RUN ./gradlew build -x check && ./gradlew deploy && rm -rf build

WORKDIR /root
VOLUME ["${CLARA_HOME}/data/input", "${CLARA_HOME}/data/output", "${CLARA_HOME}/log"]

EXPOSE 7771 7772 7773 7775


FROM openjdk:8-jre-slim

ENV CLARA_HOME="/usr/local/clara"
ENV PATH="${CLARA_HOME}/bin:${PATH}"

COPY --from=build ${CLARA_HOME} ${CLARA_HOME}

WORKDIR /root
VOLUME ["${CLARA_HOME}/data/input", "${CLARA_HOME}/data/output", "${CLARA_HOME}/log"]

EXPOSE 7771 7772 7773 7775
