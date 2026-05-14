name := "spark-essentials"

version := "0.3"

scalaVersion := "2.13.17"

val sparkVersion = "4.1.1"
val postgresVersion = "42.7.7"
val log4jVersion = "2.25.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion
)
