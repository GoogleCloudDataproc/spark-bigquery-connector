lazy val scala211Version = "2.11.8"
lazy val scala212Version = "2.12.8"

lazy val commonSettings = Seq(
  organization := "com.google.cloud.spark",
  name := "spark-bigquery",
  version := "0.11.0-beta",
  scalaVersion := scala211Version,
  crossScalaVersions := Seq(scala211Version, scala212Version),
  sparkVersion := "2.4.4",
  spName := "google/spark-bigquery",
  sparkComponents ++= Seq("core", "sql")
)

// TODO(pmkc): Migrate off Spark Packages for simplicity

// Default IntegrationTest config uses separate test directory, build files
lazy val ITest = config("it") extend Test

lazy val shaded = (project in file("."))
    .enablePlugins(BuildInfoPlugin)
    .configs(ITest)
    .settings(
      commonSettings,
      inConfig(ITest)(Defaults.testTasks),
      testOptions in Test := Seq(Tests.Filter(unitFilter)),
      testOptions in ITest := Seq(Tests.Filter(itFilter)),
      buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
      buildInfoPackage := "com.google.cloud.spark.bigquery")

// Exclude dependencies already on Spark's Classpath
val excludedOrgs = Seq(
  // All use commons-cli:1.4
  "commons-cli",
  // All use commons-lang:2.6
  "commons-lang",
  // Not a runtime dependency
  "com.google.auto.value",
  // All use jsr305:3.0.0
  "com.google.code.findbugs",
  // All use jackson-core:2.6.5
  "com.fasterxml.jackson.core",
  // Spark Uses 2.9.9 google-cloud-core uses 2.9.2
  "joda-time",
  // All use httpclient:4.3.6
  "org.apache.httpcomponents",
  // All use jackson-core-asl:1.9.13
  "org.codehaus.jackson",
  // All use SLF4J 
  "org.slf4j",
  "com.sun.jdmk",
  "com.sun.jmx",
  "javax.jms"
)
val myPackage = "com.google.cloud.spark.bigquery"

// Run scalastyle automatically
(test in Test) := ((test in Test) dependsOn scalastyle.in(Test).toTask("")).value

parallelExecution in ITest := false
val relocationPrefix = s"$myPackage.repackaged"

libraryDependencies ++= Seq(
  // Keep com.google.cloud dependencies in sync
  "com.google.cloud" % "google-cloud-bigquery" % "1.102.0",
  "com.google.cloud" % "google-cloud-bigquerystorage" % "0.120.0-beta",
  // Keep in sync with com.google.cloud
  "io.grpc" % "grpc-netty-shaded" % "1.24.1",
  "com.google.guava" % "guava" % "28.1-jre",

  "org.apache.avro" % "avro" % "1.8.2",

  // Brings SLF4J 1.7.25. Spark 2.2 has 1.7.16
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",

  // runtime
  "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop2-2.0.0" % "runtime",

  // test
  "org.scalatest" %% "scalatest" % "3.1.0" % "test",
  "org.mockito" %% "mockito-scala-scalatest" % "1.10.0" % "test")
    .map(_.excludeAll(excludedOrgs.map(ExclusionRule(_)): _*))
val renamed = Seq(
  "avro.shaded",
  "com.google",
  "com.thoughtworks.paranamer",
  "com.typesafe",
  "io.grpc",
  "io.opencensus",
  "io.perfmark",
  "org.apache.avro",
  "org.apache.commons",
  "org.checkerframework",
  "org.codehaus.mojo",
  "org.json",
  "org.threeten",
  "org.tukaani.xz",
  "org.xerial.snappy")
val notRenamed = Seq(myPackage)

def unitFilter(name: String): Boolean = (name endsWith "Suite") && !itFilter(name)

def itFilter(name: String): Boolean = name endsWith "ITSuite"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyShadeRules in assembly := (
    // Rename preserved prefixes to themselves first to keep them unchanged
    notRenamed.map(prefix => ShadeRule.rename(s"$prefix**" -> s"$prefix@1"))
        ++: renamed.map(prefix => ShadeRule.rename(s"$prefix**" -> s"$relocationPrefix.$prefix@1"))
    ).map(_.inAll)

assemblyMergeStrategy in assembly := {
  case PathList(ps@_*) if ps.last.endsWith(".properties") => MergeStrategy.filterDistinctLines
  case PathList(ps@_*) if ps.last.endsWith(".proto") => MergeStrategy.discard
  // Relocate netty-tcnative.so. This is necessary even though gRPC shades it, because we shade
  // gRPC.
  case PathList("META-INF", "native", f) if f.contains("netty_tcnative") => RelocationMergeStrategy(
    path => path.replace("native/lib", s"native/lib${relocationPrefix.replace('.', '_')}_"))

  // Relocate GRPC service registries
  case PathList("META-INF", "services", _) => ServiceResourceMergeStrategy(renamed,
    relocationPrefix)
  case x => (assemblyMergeStrategy in assembly).value(x)
}

homepage := Some(url("https://github.com/GoogleCloudPlatform/spark-bigquery-connector"))
scmInfo := Some(ScmInfo(url("https://github.com/GoogleCloudPlatform/spark-bigquery-connector"),
  "git@github.com:GoogleCloudPlatform/spark-bigquery-connector.git"))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
publishMavenStyle := true

// publish the assembly
artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("shaded"))
}

addArtifact(artifact in (Compile, assembly), assembly)

pomExtra :=
  <developers>
    <developer>
      <organization>Google LLC</organization>
      <organizationUrl>http://www.google.com</organizationUrl>
    </developer>
  </developers>

publishTo := Some(
  if (version.value.trim.endsWith("SNAPSHOT"))
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)
