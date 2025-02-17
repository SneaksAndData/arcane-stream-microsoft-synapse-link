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
    && cp -R /usr/lib/$cli_arch-linux-gnu/libz.so.* /extras \
    && cp -R /usr/lib/$cli_arch-linux-gnu/libzstd.so.* /extras

FROM gcr.io/distroless/base-debian12 AS build-release-stage
ARG USERLIB

COPY --from=build /build/target/graalvm-native-image /app
COPY --from=build-extras /extras /usr/lib/$USERLIB

WORKDIR /app

CMD ["arcane-stream-microsoft-synapse-link"]
