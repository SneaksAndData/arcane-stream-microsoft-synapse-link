FROM sbtscala/scala-sbt:graalvm-community-22.0.1_1.10.7_3.6.3 AS build
ARG GITHUB_TOKEN
ENV GITHUB_TOKEN=${GITHUB_TOKEN}

WORKDIR /build

COPY . .

RUN echo $GITHUB_TOKEN
RUN sbt graalvm-native-image:packageBin && file target/graalvm-native-image/arcane-stream-microsoft-synapse-link

FROM debian:bookworm-slim AS build-extras
ARG TARGETARCH


RUN cli_arch=$(test "$TARGETARCH" = "amd64" && echo "x86_64" || echo "aarch64") \
    && mkdir /extras \
    && cp -R /usr/lib/$TARGETARCH-linux-gnu/*.so /extras \
    && cp -R /usr/lib/$TARGETARCH-linux-gnu/*.so.* /extras

FROM gcr.io/distroless/base-debian12 AS build-release-stage
ARG TARGETARCH

COPY --from=build /build/target/graalvm-native-image /app
COPY --from=build-extras /extras /usr/lib/$TARGETARCH-linux-gnu

WORKDIR /app

CMD ["arcane-stream-microsoft-synapse-link"]
