## Microsoft Synapse Link Streaming Plugin for Arcane
This repository contains implementation of Microsoft Synapse Link (Incremental CSV - Append-Only) streaming application. Use this app to livestream your 
Dynamics 365 Finance & Operations entities to Iceberg tables, backed by Trino as streaming batch merge consumer.

### Quickstart

TBD

### Development

Project uses `Scala 3.6.1` and tested on JDK 21. When using GraalVM, use JDK 22 version. 

Plugin supports `GraalVM` native image builds. Each PR must be run with `-agentlib:native-image-agent=config-merge-dir=./configs` on a [GraalVM-CE JDK](https://sdkman.io/jdks/#graalce) in order to capture native image settings updates, if any.
