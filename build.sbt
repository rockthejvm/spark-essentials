// UPDATE: Upgraded to Spark 4.1.1, Scala 2.13.16, removed dead resolvers (Bintray shut down 2021), bumped all deps
name := "spark-essentials"

version := "0.3"

scalaVersion := "2.13.16"

val sparkVersion = "4.1.1"
val postgresVersion = "42.7.4"
val log4jVersion = "2.24.3"

// UPDATE: Removed resolvers block — bintray-spark-packages is dead, Typesafe and MavenRepository are unnecessary (Maven Central is included by default)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion
)
