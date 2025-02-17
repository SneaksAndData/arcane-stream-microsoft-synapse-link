FROM sbtscala/scala-sbt:graalvm-community-22.0.1_1.10.7_3.6.3 AS build
ARG GITHUB_TOKEN
ENV GITHUB_TOKEN=${GITHUB_TOKEN}

WORKDIR /build

COPY . .

RUN echo $GITHUB_TOKEN
RUN sbt graalvm-native-image:packageBin && file target/graalvm-native-image/arcane-stream-microsoft-synapse-link

FROM gcr.io/distroless/base-debian12 AS build-release-stage

COPY --from=build /build/target/graalvm-native-image /app

WORKDIR /app

CMD ["arcane-stream-microsoft-synapse-link"]
