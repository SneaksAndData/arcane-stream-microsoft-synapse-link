import com.typesafe.sbt.packager.graalvmnativeimage.GraalVMNativeImagePlugin.autoImport.GraalVMNativeImage

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / trackInternalDependencies := TrackLevel.TrackIfMissing
ThisBuild / exportJars := true
ThisBuild / scalaVersion := "3.6.1"
ThisBuild / organization := "com.sneaksanddata"

resolvers += "Arcane framework repo" at "https://maven.pkg.github.com/SneaksAndData/arcane-framework-scala"

credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "_",
  sys.env("GITHUB_TOKEN")
)

enablePlugins(GraalVMNativeImagePlugin)

mainClass := Some("com.sneaksanddata.arcane.microsoft_synapse_link.main")
GraalVMNativeImage / mainClass := Some("com.sneaksanddata.arcane.microsoft_synapse_link.main")

lazy val plugin = (project in file("."))
  .settings(
    name := "arcane-stream-microsoft-synapse-link",
    idePackagePrefix := Some("com.sneaksanddata.arcane.microsoft_synapse_link"),

    libraryDependencies += "com.sneaksanddata" % "arcane-framework_3" % "0.3.7",
    libraryDependencies += "io.netty" % "netty-tcnative-boringssl-static" % "2.0.65.Final",
    // https://mvnrepository.com/artifact/io.trino/trino-jdbc
    libraryDependencies += "io.trino" % "trino-jdbc" % "465",

    // Azure dependencies
    // https://mvnrepository.com/artifact/com.azure/azure-storage-blob
    libraryDependencies += "com.azure" % "azure-storage-blob" % "12.29.1",
    // https://mvnrepository.com/artifact/com.azure/azure-identity
    libraryDependencies += "com.azure" % "azure-identity" % "1.15.3",
    // Jackson pin
    libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.1",
    libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.18.1",
    libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.18.1",
    libraryDependencies += "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.18.1",

    // Test dependencies
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    libraryDependencies += "org.scalatest" %% "scalatest-flatspec" % "3.2.19" % Test,

    graalVMNativeImageOptions ++= Seq(
      "--no-fallback",
      "-O2",
      "--initialize-at-run-time=okhttp3.internal.platform.Android10Platform,reactor.util.Metrics,org.bouncycastle,io.netty",
      "--enable-http",
      "--enable-https",
      "--verbose",
      "-H:+UnlockExperimentalVMOptions",
      "-H:+AddAllCharsets",
      "-H:+ReportExceptionStackTraces",
      // enable this if you experience build errors to find the root cause
      //"-H:+PrintClassInitialization",
      "-H:ResourceConfigurationFiles=../../configs/resource-config.json",
      "-H:ReflectionConfigurationFiles=../../configs/reflect-config.json",
      "-H:JNIConfigurationFiles=../../configs/jni-config.json",
      "-H:DynamicProxyConfigurationFiles=../../configs/proxy-config.json",
      "-H:SerializationConfigurationFiles=../../configs/serialization-config.json",
      "--exclude-config", "azure-core-1.55.2.jar", ".*.properties"
    ),

    assembly / mainClass := Some("com.sneaksanddata.arcane.microsoft_synapse_link.main"),

    // We do not use the version name here, because it's executable file name
    // and we want to keep it consistent with the name of the project
    assembly / assemblyJarName := "com.sneaksanddata.arcane.microsoft-synapse-link.assembly.jar",

    assembly / assemblyMergeStrategy := {
      case "NOTICE" => MergeStrategy.discard
      case "LICENSE" => MergeStrategy.discard
      case ps if ps.contains("META-INF/services/java.net.spi.InetAddressResolverProvider") => MergeStrategy.discard
      case ps if ps.contains("META-INF/services/") => MergeStrategy.concat("\n")
      case ps if ps.startsWith("META-INF/native") => MergeStrategy.first

      // Removes duplicate files from META-INF
      // Mostly io.netty.versions.properties, license files, INDEX.LIST, MANIFEST.MF, etc.
      case ps if ps.startsWith("META-INF") => MergeStrategy.discard
      case ps if ps.endsWith("logback.xml") => MergeStrategy.discard
      case ps if ps.endsWith("module-info.class") => MergeStrategy.discard
      case ps if ps.endsWith("package-info.class") => MergeStrategy.discard

      // for javax.activation package take the first one
      case PathList("javax", "activation", _*) => MergeStrategy.last

      // For other files we use the default strategy (deduplicate)
      case x => MergeStrategy.deduplicate
    }
  )
