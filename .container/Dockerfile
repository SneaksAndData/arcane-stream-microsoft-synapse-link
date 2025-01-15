FROM --platform=$BUILDPLATFORM sbtscala/scala-sbt:eclipse-temurin-23.0.1_11_1.10.5_3.5.2 AS build
ARG GITHUB_TOKEN
ENV GITHUB_TOKEN=${GITHUB_TOKEN}

WORKDIR /build

COPY . .

RUN echo $GITHUB_TOKEN
RUN sbt assembly

FROM --platform=$BUILDPLATFORM gcr.io/distroless/java21-debian12

COPY --from=build /build/target/scala-3.6.1 /app

WORKDIR /app

CMD ["com.sneaksanddata.arcane.microsoft-synapse-link.assembly.jar"]
