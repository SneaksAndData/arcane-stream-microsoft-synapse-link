FROM sbtscala/scala-sbt:graalvm-community-22.0.1_1.10.7_3.6.3 AS build
ARG GITHUB_TOKEN
ENV GITHUB_TOKEN=${GITHUB_TOKEN}

WORKDIR /build

COPY . .

RUN echo $GITHUB_TOKEN
RUN sbt graalvm-native-image:packageBin && file target/graalvm-native-image/arcane-stream-microsoft-synapse-link

FROM debian:bookworm-slim AS build-extras

FROM gcr.io/distroless/base-debian12 AS build-release-stage
ARG TARGETARCH

RUN cli_arch=$(test "$TARGETARCH" = "amd64" && echo "x86_64" || echo "aarch64")

COPY --from=build /build/target/graalvm-native-image /app
COPY --from=build-extras /usr/lib/$TARGETARCH-linux-gnu/libz.so.1 /usr/lib/$TARGETARCH-linux-gnu
COPY --from=build-extras /usr/lib/$TARGETARCH-linux-gnu/libz.so.1.2.13 /usr/lib/$TARGETARCH-linux-gnu
COPY --from=build-extras /usr/lib/$TARGETARCH-linux-gnu/libzstd.so.1 /usr/lib/$TARGETARCH-linux-gnu
COPY --from=build-extras /usr/lib/$TARGETARCH-linux-gnu/libzstd.so.1.5.4 /usr/lib/$TARGETARCH-linux-gnu

WORKDIR /app

CMD ["arcane-stream-microsoft-synapse-link"]
