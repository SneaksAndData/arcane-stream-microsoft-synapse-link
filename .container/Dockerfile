FROM --platform=$BUILDPLATFORM sbtscala/scala-sbt:eclipse-temurin-25.0.1_8_1.12.9_3.8.3 AS build
ARG GITHUB_TOKEN
ENV GITHUB_TOKEN=${GITHUB_TOKEN}

WORKDIR /build

COPY . .

RUN sbt assembly

FROM gcr.io/distroless/java25-debian13

COPY --from=build /build/target/scala-3.6.1 /app

WORKDIR /app

CMD ["com.sneaksanddata.arcane.microsoft-synapse-link.assembly.jar"]
