ThisBuild / version := "2.5.0-SNAPSHOT"
ThisBuild / trackInternalDependencies := TrackLevel.TrackIfMissing
ThisBuild / exportJars := true
ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "com.sneaksanddata"

resolvers += "Arcane framework repo" at "https://maven.pkg.github.com/SneaksAndData/arcane-framework-scala"

val githubToken: Option[String] =
  sys.env.get("GITHUB_TOKEN").orElse {
    val tokenFile = file(".ghtoken")
    if (tokenFile.exists()) {
      val source = scala.io.Source.fromFile(tokenFile)
      try Some(source.mkString.trim)
      finally source.close()
    } else None
  }

credentials ++= githubToken.map { token =>
  Credentials(
    "GitHub Package Registry",
    "maven.pkg.github.com",
    "_",
    token
  )
}.toSeq

mainClass := Some("com.sneaksanddata.arcane.microsoft_synapse_link.main")

lazy val plugin = (project in file("."))
  .settings(
    name := "arcane-stream-microsoft-synapse-link",
    idePackagePrefix := Some("com.sneaksanddata.arcane.microsoft_synapse_link"),

    libraryDependencies += "com.sneaksanddata" % "arcane-framework_3" % "2.3.2-56-gfdb482d",
    libraryDependencies += "io.netty" % "netty-tcnative-boringssl-static" % "2.0.74.Final",

    // Test dependencies
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.20" % Test,
    libraryDependencies += "org.scalatest" %% "scalatest-flatspec" % "3.2.20" % Test,
    libraryDependencies += "dev.zio" %% "zio-test"          % "2.1.27" % Test,
    libraryDependencies += "dev.zio" %% "zio-test-sbt"      % "2.1.27" % Test,
    libraryDependencies += "com.sneaksanddata" % "arcane-framework-test_3" % "0.3.1-2-g404ebe4" % Test,


    assembly / mainClass := Some("com.sneaksanddata.arcane.microsoft_synapse_link.main"),

    // Put JAR in <root>/target/ directly. sbt 2's `target` is the versioned per-project output
    // directory (`target/out/jvm/scala-x.x.x/<project>`), so derive the path from the build root instead.
    assembly / assemblyOutputPath := (ThisBuild / baseDirectory).value / "target" / (assembly / assemblyJarName).value,

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
      case PathList("javax", "xml", _*) => MergeStrategy.last

      // Hadoop/Iceberg transitive deps bundle duplicate servlet and javax.inject APIs
      case PathList("javax", "servlet", _*) => MergeStrategy.first
      case PathList("javax", "inject", _*) => MergeStrategy.first
      case PathList("org", "aopalliance", _*) => MergeStrategy.first

      // Hadoop bundles a shaded copy of jspecify annotations; javax.annotation is shipped by multiple jars
      case PathList("org", "jspecify", _*) => MergeStrategy.first
      case PathList("javax", "annotation", _*) => MergeStrategy.first

      // For other files we use the default strategy (deduplicate)
      case x => MergeStrategy.deduplicate
    }
  )
