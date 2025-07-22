name := "spark-essentials"

version := "0.2"

scalaVersion := "2.13.16"

enablePlugins(ScalafmtPlugin)
scalafmtOnCompile := true

val sparkVersion = "3.5.6"
val postgresVersion = "42.6.0"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.postgresql" % "postgresql" % postgresVersion,
  "org.apache.hadoop" % "hadoop-client-api" % "3.3.4",
  "org.apache.hadoop" % "hadoop-client-runtime" % "3.3.4"
)
