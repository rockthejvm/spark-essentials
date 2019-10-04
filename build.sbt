name := "spark-essentials"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.3"
val vegasVersion = "0.3.11"
val postgresVersion = "42.2.2"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "http://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  // vegas
  "org.vegas-viz" %% "vegas" % vegasVersion,
  "org.vegas-viz" %% "vegas-spark" % vegasVersion,
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion
)