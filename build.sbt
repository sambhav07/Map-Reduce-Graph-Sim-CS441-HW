ThisBuild / version := "0.1.0-SNAPSHOT"

Global / excludeLintKeys += idePackagePrefix
Global / excludeLintKeys += test / fork
Global / excludeLintKeys += run / mainClass

val scalaTestVersion = "3.2.11"
val guavaVersion = "31.1-jre"
val typeSafeConfigVersion = "1.4.2"
val logbackVersion = "1.2.10"
val sfl4sVersion = "2.0.0-alpha5"
val graphVizVersion = "0.18.1"
val netBuddyVersion = "1.14.4"
val catsVersion = "2.9.0"
val apacheCommonsVersion = "2.13.0"
val jGraphTlibVersion = "1.5.2"
val scalaParCollVersion = "1.0.4"
val guavaAdapter2jGraphtVersion = "1.5.2"

lazy val commonDependencies = Seq(
  "org.scala-lang.modules" %% "scala-parallel-collections" % scalaParCollVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "org.scalatestplus" %% "mockito-4-2" % "3.2.12.0-RC2" % Test,
  "com.typesafe" % "config" % typeSafeConfigVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "net.bytebuddy" % "byte-buddy" % netBuddyVersion
)

lazy val root = (project in file("."))
  .settings(
    scalaVersion := "3.2.2",
    name := "NetGameSim",
    idePackagePrefix := Some("com.lsc"),
    libraryDependencies ++= commonDependencies
  ).aggregate(NetModelGenerator,GenericSimUtilities).dependsOn(NetModelGenerator)

lazy val NetModelGenerator = (project in file("NetModelGenerator"))
  .settings(
    scalaVersion := "3.2.2",
    name := "NetModelGenerator",
    libraryDependencies ++= commonDependencies ++ Seq(
      "com.google.guava" % "guava" % guavaVersion,
      "guru.nidi" % "graphviz-java" % graphVizVersion,
      "org.typelevel" %% "cats-core" % catsVersion,
      "commons-io" % "commons-io" % apacheCommonsVersion,
      "org.jgrapht" % "jgrapht-core" % jGraphTlibVersion,
      "org.jgrapht" % "jgrapht-guava" % guavaAdapter2jGraphtVersion,
      "org.apache.hadoop" % "hadoop-common" % "3.3.6",
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.6",
      "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.3.6",
      "log4j" % "log4j" % "1.2.17",
      "org.yaml" % "snakeyaml" % "1.29",
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.13.1",
      "software.amazon.awssdk" % "s3" % "2.17.52" // use the latest version
    )
  ).dependsOn(GenericSimUtilities)

lazy val GenericSimUtilities = (project in file("GenericSimUtilities"))
  .settings(
    scalaVersion := "3.2.2",
    name := "GenericSimUtilities",
    libraryDependencies ++= commonDependencies
  )


scalacOptions ++= Seq(
      "-deprecation", // emit warning and location for usages of deprecated APIs
      "--explain-types", // explain type errors in more detail
      "-feature" // emit warning and location for usages of features that should be imported explicitly
    )

compileOrder := CompileOrder.JavaThenScala
test / fork := true
run / fork := true
run / javaOptions ++= Seq(
  "-Xms8G",
  "-Xmx100G",
  "-XX:+UseG1GC"
)

Compile / mainClass := Some("com.lsc.MyMain")
run / mainClass := Some("com.lsc.MyMain")

val jarName = "netmodelsim.jar"
assembly/assemblyJarName := jarName


//Merging strategies
ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}



//libraryDependencies ++= Seq(
//  "org.apache.hadoop" % "hadoop-common" % "3.3.6",
//  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.6",
//  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.3.6",
//  "log4j" % "log4j" % "1.2.17",
//  "org.yaml" % "snakeyaml" % "1.29",
//  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.13.1"
//).map(_.exclude("org.slf4j", "slf4j-log4j12")) // exclude slf4j-log4j12 from all dependencies

//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case x => MergeStrategy.first
//}
