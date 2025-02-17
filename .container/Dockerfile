FROM --platform=$BUILDPLATFORM ghcr.io/graalvm/native-image-community:21 AS build
ARG GITHUB_TOKEN
ENV GITHUB_TOKEN=${GITHUB_TOKEN}

WORKDIR /build

COPY . .

RUN echo $GITHUB_TOKEN
RUN sbt graalvm-native-image:packageBin

FROM gcr.io/distroless/base-debian12

COPY --from=build /build/target/graalvm-native-image /app

WORKDIR /app

CMD ["arcane-stream-microsoft-synapse-link"]
