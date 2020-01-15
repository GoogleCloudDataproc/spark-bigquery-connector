lazy val scala211Version = "2.11.8"
lazy val scala212Version = "2.12.8"

organization in ThisBuild := "com.google.cloud.spark"
version in ThisBuild := "0.11.3-beta-SNAPSHOT"
scalaVersion in ThisBuild := scala211Version
crossScalaVersions in ThisBuild := Seq(scala211Version, scala212Version)
sparkVersion in ThisBuild := "2.4.4"
spName in ThisBuild := "google/spark-bigquery"

// For https://github.com/GoogleCloudPlatform/spark-bigquery-connector/issues/72
// Based on
// https://github.com/sbt/sbt-assembly/#q-despite-the-concerned-friends-i-still-want-publish-fat-jars-what-advice-do-you-have
lazy val root = (project in file("."))
  .disablePlugins(AssemblyPlugin)
  .aggregate(connector, fatJar, all)

lazy val connector = project.in(file("connector"))

lazy val fatJar = project
  .enablePlugins(AssemblyPlugin)
  .settings(
    organization := (organization in ThisBuild).value,
    version := (version in ThisBuild).value,
    scalaVersion := (scalaVersion in ThisBuild).value,
    crossScalaVersions := (crossScalaVersions in ThisBuild).value,
    skip in publish := true,
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assemblyShadeRules in assembly := (
      notRenamed.map(prefix => ShadeRule.rename(s"$prefix**" -> s"$prefix@1"))
        ++: renamed.map(prefix => ShadeRule.rename(s"$prefix**" -> s"$relocationPrefix.$prefix@1"))
      ).map(_.inAll),

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
  )
  .dependsOn(connector)


lazy val all = project
  .settings(
    name := "spark-bigquery-with-deps",
    packageBin in Compile := (assembly in(fatJar, Compile)).value
  )

lazy val myPackage = "com.google.cloud.spark.bigquery"
lazy val relocationPrefix = s"$myPackage.repackaged"

lazy val renamed = Seq(
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
  "org.conscrypt",
  "org.json",
  "org.threeten",
  "org.tukaani.xz",
  "org.xerial.snappy")
lazy val notRenamed = Seq(myPackage)

homepage := Some(url("https://github.com/GoogleCloudPlatform/spark-bigquery-connector"))
scmInfo := Some(ScmInfo(url("https://github.com/GoogleCloudPlatform/spark-bigquery-connector"),
  "git@github.com:GoogleCloudPlatform/spark-bigquery-connector.git"))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
publishMavenStyle := true

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
