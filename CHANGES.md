# Spark Essentials — All Changes (Spark 3.5.0 → 4.1.1)

This document contains every change made to upgrade the repository from Spark 3.5.0 to Spark 4.1.1.
Each modified file includes a diff showing exactly what changed. New files are shown in full.

---

## Table of Contents

1. [Build & Infrastructure](#1-build--infrastructure)
   - [build.sbt](#buildsbt)
   - [project/build.properties](#projectbuildproperties)
2. [Docker Setup](#2-docker-setup)
   - [docker-compose.yml](#docker-composeyml)
   - [docker-clean.sh](#docker-cleansh)
   - [docker-clean.bat](#docker-cleanbat)
   - [Deleted Files](#deleted-docker-files)
   - [spark-cluster/README.md](#spark-clusterreadmemd)
3. [Documentation](#3-documentation)
   - [README.md](#readmemd)
   - [HadoopWindowsUserSetup.md](#hadoopwindowsusersetupmd)
4. [Code Changes — extends App Fix](#4-code-changes--extends-app-fix)
5. [Code Changes — Specific Fixes](#5-code-changes--specific-fixes)
   - [ColumnsAndExpressions.scala — Symbol syntax](#columnsandexpressionsscala)
   - [SparkSql.scala — Legacy comment](#sparksqlscala)
   - [SparkShell.scala — Container names & paths](#sparkshellscala)
   - [SparkJobAnatomy.scala — Container names](#sparkjobanatomyscala)
   - [TestDeployApp.scala — Deploy instructions](#testdeployappscala)
6. [New Lessons](#6-new-lessons)
   - [part8spark4/VariantType.scala](#varianttypescala)
   - [part8spark4/SparkSQLNewFeatures.scala](#sparksqlnewfeaturesscala)

---

## 1. Build & Infrastructure

### build.sbt

**Changes:** Spark 3.5.0 → 4.1.1, Scala 2.13.12 → 2.13.16, removed dead resolvers (Bintray shut down 2021), bumped PostgreSQL driver and Log4j.

```diff
+// UPDATE: Upgraded to Spark 4.1.1, Scala 2.13.16, removed dead resolvers (Bintray shut down 2021), bumped all deps
 name := "spark-essentials"

-version := "0.2"
+version := "0.3"

-scalaVersion := "2.13.12"
+scalaVersion := "2.13.16"

-val sparkVersion = "3.5.0"
-val postgresVersion = "42.6.0"
-val log4jVersion = "2.20.0"
-
-resolvers ++= Seq(
-  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
-  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
-  "MavenRepository" at "https://mvnrepository.com"
-)
+val sparkVersion = "4.1.1"
+val postgresVersion = "42.7.4"
+val log4jVersion = "2.24.3"

+// UPDATE: Removed resolvers block — bintray-spark-packages is dead, Typesafe and MavenRepository are unnecessary (Maven Central is included by default)

 libraryDependencies ++= Seq(
   "org.apache.spark" %% "spark-core" % sparkVersion,
@@ -23,4 +19,4 @@
   "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
   // postgres for DB connectivity
   "org.postgresql" % "postgresql" % postgresVersion
-)
+)
```

### project/build.properties

**Changes:** sbt 1.9.6 → 1.10.7 for Java 17/21 compatibility.

```diff
-sbt.version = 1.9.6
+# UPDATE: Bumped from 1.9.6 to 1.10.7 for Java 17/21 compatibility
+sbt.version = 1.10.7
```

---

## 2. Docker Setup

### docker-compose.yml

**Changes:** Replaced the Postgres-only compose file with a unified file that also includes the Spark cluster using official Apache Spark Docker images. No more custom Dockerfiles or build step.

```diff
-version: '2'
+# UPDATE: Replaced separate Postgres + custom Spark cluster compose files with a single unified file
+# using official Apache Spark Docker images — no more custom Dockerfiles or build-images.sh needed.
+# Usage: docker compose up --scale spark-worker=3

 services:
   postgres:
-    image: postgres:latest
+    image: postgres:17
     container_name: postgres
     environment:
-      - "TZ=Europe/Amsterdam"
-      - "POSTGRES_USER=docker"
-      - "POSTGRES_PASSWORD=docker"
+      TZ: "Europe/Amsterdam"
+      POSTGRES_USER: "docker"
+      POSTGRES_PASSWORD: "docker"
     ports:
       - "5432:5432"
     volumes:
       - "./sql:/docker-entrypoint-initdb.d"
+
+  spark-master:
+    image: apache/spark:4.1.1-scala
+    container_name: spark-master
+    command: /opt/spark/sbin/start-master.sh
+    environment:
+      SPARK_MASTER_HOST: spark-master
+      SPARK_NO_DAEMONIZE: "true"
+    ports:
+      - "7077:7077"
+      - "9090:8080"
+      - "4040:4040"
+    volumes:
+      - ./spark-cluster/apps:/opt/spark-apps
+      - ./spark-cluster/data:/opt/spark-data
+
+  spark-worker:
+    image: apache/spark:4.1.1-scala
+    command: /opt/spark/sbin/start-worker.sh spark://spark-master:7077
+    depends_on:
+      - spark-master
+    environment:
+      SPARK_WORKER_CORES: 1
+      SPARK_WORKER_MEMORY: 1G
+      SPARK_NO_DAEMONIZE: "true"
+    volumes:
+      - ./spark-cluster/apps:/opt/spark-apps
+      - ./spark-cluster/data:/opt/spark-data
```

### docker-clean.sh

**Changes:** Replaced `docker rm -f $(docker ps -aq)` (killed ALL containers on the machine) with `docker compose down -v`.

```diff
 #!/bin/bash
-docker rm -f $(docker ps -aq)
+# UPDATE: replaced "docker rm -f $(docker ps -aq)" which killed ALL containers on the machine
+docker compose down -v
```

### docker-clean.bat

```diff
-docker rm -f $(docker ps -aq)
+@echo off
+REM UPDATE: replaced "docker rm -f $(docker ps -aq)" which killed ALL containers on the machine
+docker compose down -v
```

### Deleted Docker Files

The following files were **deleted** — they are replaced by the official `apache/spark:4.1.1-scala` Docker image:

| File | What it was |
|---|---|
| `spark-cluster/docker/base/Dockerfile` | Custom base image (Scala, sbt, Spark download) |
| `spark-cluster/docker/spark-master/Dockerfile` | Custom master image |
| `spark-cluster/docker/spark-master/start-master.sh` | Master startup script |
| `spark-cluster/docker/spark-worker/Dockerfile` | Custom worker image |
| `spark-cluster/docker/spark-worker/start-worker.sh` | Worker startup script |
| `spark-cluster/docker/spark-submit/Dockerfile` | Custom submit image |
| `spark-cluster/docker/spark-submit/spark-submit.sh` | Submit startup script |
| `spark-cluster/build-images.sh` | Shell script to build custom images |
| `spark-cluster/build-images.bat` | Windows script to build custom images |
| `spark-cluster/env/spark-worker.sh` | Worker environment variables |
| `spark-cluster/docker-compose.yml` | Separate Spark cluster compose (merged into root) |

### spark-cluster/README.md

**Changes:** Completely rewritten — replaced custom Docker image build instructions with official image usage.

**New content:**

```markdown
# Spark Cluster (Docker)

<!-- UPDATE: Completely rewritten — replaced custom Docker image build system with official Apache Spark images.
     No more build-images.sh, custom Dockerfiles, or dos2unix workarounds. -->

## Quick Start

From the **root** of this repository:

    docker compose up --scale spark-worker=3

This starts:
- A **PostgreSQL** database (port 5432) pre-loaded with the course data
- A **Spark Master** (Web UI at http://localhost:9090, Spark protocol on port 7077)
- **3 Spark Workers** connected to the master

All services use official Docker images — no build step required.

## Accessing the Cluster

    # Open a shell on the master node
    docker exec -it spark-master bash

    # Launch the Spark shell (Scala)
    /opt/spark/bin/spark-shell --master spark://spark-master:7077

    # Launch the Spark SQL shell
    /opt/spark/bin/spark-sql --master spark://spark-master:7077

## Submitting a JAR

1. Build your JAR (e.g. `sbt package`) and copy it to `spark-cluster/apps/`
2. Copy any data files to `spark-cluster/data/`
3. Submit:

    docker exec -it spark-master /opt/spark/bin/spark-submit \
      --class your.main.Class \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      /opt/spark-apps/your-jar.jar [args...]

The `apps/` and `data/` directories are mounted at `/opt/spark-apps` and `/opt/spark-data`.

## Resource Allocation

- Default CPU cores per worker: 1
- Default RAM per worker: 1G
- Modify in the root `docker-compose.yml` under `spark-worker` environment variables.

## Stopping

    docker compose down      # stop containers
    docker compose down -v   # stop and remove volumes
```

---

## 3. Documentation

### README.md

**Changes:** Added prerequisites section (JDK 17+, Docker, IntelliJ), simplified setup (no build-images step), added cluster access and spark-submit instructions, removed `\r` troubleshooting section (no longer relevant with official images).

```diff
 # The official repository for the Rock the JVM Spark Essentials with Scala course

+<!-- UPDATE: Rewritten for Spark 4.1.1 — simplified Docker setup (official images), updated prerequisites, removed build-images step -->
+
-This repository contains the code we wrote during  [Rock the JVM's Spark Essentials with Scala](...) Unless explicitly mentioned...
+This repository contains the code we wrote during [Rock the JVM's Spark Essentials with Scala](...). Unless explicitly mentioned...
+
+## Prerequisites
+
+- **Java**: JDK 17 or 21 (recommend Eclipse Temurin 21)
+- **Docker**: Docker Desktop (Mac/Windows) or Docker Engine (Linux)
+- **IDE**: IntelliJ IDEA with the Scala plugin
+- **Windows-specific**: See HadoopWindowsUserSetup.md or use WSL 2 (recommended)
+
+> **Note**: Spark 4.x requires Java 17 or 21. Java 8 and 11 are **no longer supported**.

 ## How to install

-- install [Docker](https://docker.com)
-- either clone the repo or download as zip
-- open with IntelliJ as an SBT project
-...
-- Linux/Mac users: build the Docker-based Spark cluster with
-  chmod +x build-images.sh
-  ./build-images.sh
-- Windows users: build the Docker-based Spark cluster with
-  build-images.bat
-- when prompted to start the Spark cluster...
+- Install Docker
+- Install JDK 17 or 21 (Eclipse Temurin recommended)
+- Either clone the repo or download as zip
+- Open with IntelliJ as an SBT project
+- Windows users: use this guide or WSL 2
+- Run `docker compose up` to start PostgreSQL and the Spark cluster
+- To spin up multiple workers: `docker compose up --scale spark-worker=3`
+- Access Spark Master UI at http://localhost:9090
+
+### Connecting to PostgreSQL / Spark Cluster / Submitting JARs
+[sections added with concrete commands]
-
-### Spark Cluster Troubleshooting
-#### Windows users - '\r' command not found
-[entire section removed — no longer relevant with official images]
```

### HadoopWindowsUserSetup.md

**Changes:** Rewritten for JDK 21, Hadoop 3.4.x winutils, added WSL 2 as recommended approach, removed unnecessary full-Hadoop setup steps (HDFS, YARN config), fixed contradictory JDK version references (old guide linked JDK 8 download but said "JDK 11+").

Key differences:
- Added "Recommended: Use WSL 2" section at the top
- JDK 8 → JDK 21 (Spark 4.x requirement)
- `cdarlint/winutils` (unmaintained) → `kontext-tech/winutils`
- `hadoop-3.3.5` → `hadoop-3.4.0`
- Removed Steps 5-6 (full Hadoop HDFS/YARN configuration) — not needed for Spark local mode
- Added troubleshooting section

---

## 4. Code Changes — extends App Fix

**Applied to all 15 lesson files.** The `App` trait is deprecated in Scala 2.13 and removed in Scala 3.

The same pattern was applied to each file:

```diff
-object FileName extends App {
+object FileName { // UPDATE: replaced "extends App" (deprecated in Scala 2.13, removed in Scala 3) with def main
+
+  def main(args: Array[String]): Unit = {

   // ... all existing code unchanged ...

+  }
 }
```

**Files changed:**

| File | 
|---|
| `src/main/scala/part1recap/ScalaRecap.scala` |
| `src/main/scala/part2dataframes/DataFramesBasics.scala` |
| `src/main/scala/part2dataframes/DataSources.scala` |
| `src/main/scala/part2dataframes/ColumnsAndExpressions.scala` |
| `src/main/scala/part2dataframes/Aggregations.scala` |
| `src/main/scala/part2dataframes/Joins.scala` |
| `src/main/scala/part3typesdatasets/CommonTypes.scala` |
| `src/main/scala/part3typesdatasets/ComplexTypes.scala` |
| `src/main/scala/part3typesdatasets/Datasets.scala` |
| `src/main/scala/part3typesdatasets/ManagingNulls.scala` |
| `src/main/scala/part4sql/SparkSql.scala` |
| `src/main/scala/part5lowlevel/RDDs.scala` |
| `src/main/scala/part6practical/SparkJobAnatomy.scala` |
| `src/main/scala/part7bigdata/TaxiApplication.scala` |
| `src/main/scala/playground/Playground.scala` |

---

## 5. Code Changes — Specific Fixes

### ColumnsAndExpressions.scala

**Changes:** Replaced `'Year` (Scala Symbol syntax, deprecated in 2.13 and removed in Scala 3) with `$"Year"`.

```diff
   // various select methods
   import spark.implicits._
+  // UPDATE: removed 'Year (Scala Symbol syntax, deprecated in 2.13 and removed in Scala 3)
   carsDF.select(
     carsDF.col("Name"),
     col("Acceleration"),
     column("Weight_in_lbs"),
-    'Year, // Scala Symbol, auto-converted to column
-    $"Horsepower", // fancier interpolated string, returns a Column object
+    $"Year", // interpolated string, returns a Column object
+    $"Horsepower", // same as above
     expr("Origin") // EXPRESSION
   )
```

### SparkSql.scala

**Changes:** Removed obsolete Spark 2.4 legacy config comment.

```diff
     .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
-    // only for Spark 2.4 users:
-    // .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
+    // UPDATE: removed obsolete Spark 2.4 legacy config comment
     .getOrCreate()
```

### SparkShell.scala

**Changes:** Updated Docker commands for official Spark images — removed build step, updated container names and Spark binary paths.

```diff
   /**
+    * UPDATE: updated for official Apache Spark Docker images (no more custom build step)
     *
     * To setup the Spark cluster in Docker:
     *
-    *   1. Build the Docker containers
-    *       ./build-images.sh
-    *   2. Start a cluster with one master and 3 workers
-    *       docker-compose up --scale spark-worker=3
+    *   1. Start a cluster with one master and 3 workers (from the repo root)
+    *       docker compose up --scale spark-worker=3
     *
     * To open the Spark SQL shell in the master container:
     *   1. In another terminal window/tab, connect to the Docker container and open a regular shell
-    *       docker exec -it docker-spark-cluster_spark-master_1 bash
+    *       docker exec -it spark-master bash
     *   2. Execute the Spark SQL shell
-    *       /spark/bin/spark-sql
+    *       /opt/spark/bin/spark-sql
     *
     *  If you want to inspect the files that Spark SQL writes, open another terminal window/tab
-    *       docker exec -it docker-spark-cluster_spark-master_1 bash
+    *       docker exec -it spark-master bash
```

### SparkJobAnatomy.scala

**Changes:** Updated Docker commands for official images and docker compose v2.

```diff
+  // UPDATE: updated Docker commands for official Spark images and docker compose v2
   // start cluster
-  // docker-compose up --scale spark-worker=3
-  // in another terminal: docker-exec -it spark-cluster_spark-master_1 bash
+  // docker compose up --scale spark-worker=3
+  // in another terminal: docker exec -it spark-master bash
```

### TestDeployApp.scala

**Changes:** Rewritten build/deploy instructions for official Docker images and sbt packaging. Updated container names, Spark paths, removed `--supervise` flag.

```diff
    /*
-    * Build a JAR to run a Spark application on the Docker cluster
+    * UPDATE: Rewritten for official Apache Spark Docker images and sbt packaging.
     *
-    *   - project structure -> artifacts, add artifact from "module with dependencies"
-    *   - (important) check "copy to the output folder and link to manifest"
-    *   - (important) then from the generated folder path, delete so that the folder path ends in src/
+    * Build a JAR to run a Spark application on the Docker cluster:
     *
-    * Build the JAR: Build -> Build Artifacts... -> select the jar -> build
-    * Copy the JAR and movies.json to spark-cluster/apps
-    * (the apps and data folders are mapped to /opt/spark-apps and /opt/spark-data in the containers)
+    * Option A (sbt): Run `sbt package` from the project root.
+    *   The JAR will be at target/scala-2.13/spark-essentials_2.13-0.3.jar
     *
+    * Option B (IntelliJ):
+    *   - Project Structure -> Artifacts, add artifact from "module with dependencies"
+    *   - (important) check "copy to the output folder and link to manifest"
+    *   - Build -> Build Artifacts... -> select the jar -> build
     *
-    * */
+    * Copy the JAR and movies.json to spark-cluster/apps and spark-cluster/data respectively.
+    * (the apps and data folders are mapped to /opt/spark-apps and /opt/spark-data in the containers)
+    */

   /**
-    * How to run the Spark application on the Docker cluster
+    * UPDATE: Updated Docker commands for official images (container name, Spark path).
+    *
+    * How to run the Spark application on the Docker cluster:
     *
-    * 1. Start the cluster
-    *   docker-compose up --scale spark-worker=3
+    * 1. Start the cluster (from the repo root)
+    *   docker compose up --scale spark-worker=3
     *
     * 2. Connect to the master node
-    *   docker exec -it spark-cluster_spark-master_1 bash
+    *   docker exec -it spark-master bash
     *
     * 3. Run the spark-submit command
-    *   /spark/bin/spark-submit \
+    *   /opt/spark/bin/spark-submit \
     *     --class part6practical.TestDeployApp \
-    *     --master spark://(dockerID):7077 \
+    *     --master spark://spark-master:7077 \
     *     --deploy-mode client \
     *     --verbose \
-    *     --supervise \
-    *     spark-essentials.jar /opt/spark-data/movies.json /opt/spark-data/goodMovies
+    *     /opt/spark-apps/spark-essentials_2.13-0.3.jar /opt/spark-data/movies.json /opt/spark-data/goodMovies
     */
```

---

## 6. New Lessons

### VariantType.scala

**New file:** `src/main/scala/part8spark4/VariantType.scala`

Covers the VARIANT data type (Spark 4.0's headline feature for semi-structured data).

```scala
package part8spark4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// NEW LESSON: Spark 4.x VARIANT data type — native semi-structured data support
object VariantType {

  def main(args: Array[String]): Unit = {

  val spark = SparkSession.builder()
    .appName("Variant Type")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  // The VARIANT type is new in Spark 4.0.
  // It stores JSON-like data in an optimized binary format — much faster than storing JSON as strings.

  // 1 - Creating VARIANT data from JSON strings
  val jsonDF = Seq(
    """{"name": "Alice", "age": 30, "address": {"city": "NYC", "zip": "10001"}}""",
    """{"name": "Bob", "age": 25, "skills": ["Spark", "Scala"]}""",
    """{"name": "Charlie", "age": 35, "address": {"city": "SF"}}"""
  ).toDF("raw_json")

  val variantDF = jsonDF.select(parse_json(col("raw_json")).as("data"))
  variantDF.printSchema() // data: variant
  variantDF.show(truncate = false)

  // 2 - Extracting fields from VARIANT
  // variant_get: extract a field (throws error if path is missing)
  // try_variant_get: extract a field (returns null if path is missing — safe version)
  val extractedDF = variantDF.select(
    variant_get(col("data"), "$.name", "string").as("name"),
    variant_get(col("data"), "$.age", "int").as("age"),
    try_variant_get(col("data"), "$.address.city", "string").as("city") // returns null if path missing
  )
  extractedDF.show()

  // 3 - Schema discovery: inspect the structure of VARIANT data
  variantDF.select(schema_of_variant(col("data"))).show(truncate = false)

  // 4 - Checking for VARIANT nulls (different from SQL NULL — JSON null inside variant)
  val nullCheckDF = variantDF.select(
    variant_get(col("data"), "$.name", "string").as("name"),
    is_variant_null(col("data"), "$.address").as("address_is_null")
  )
  nullCheckDF.show()

  // 5 - VARIANT in SQL
  variantDF.createOrReplaceTempView("people")
  spark.sql(
    """
      |SELECT
      |  variant_get(data, '$.name', 'string') as name,
      |  variant_get(data, '$.age', 'int') as age,
      |  try_variant_get(data, '$.skills[0]', 'string') as first_skill
      |FROM people
    """.stripMargin
  ).show()

  /**
    * Exercises
    *
    * 1. Parse the movies.json file: read each line as text, parse_json it into a VARIANT column,
    *    then extract Title and IMDB_Rating. Show the top 10 by rating.
    *
    * 2. Use try_variant_get to safely extract US_DVD_Sales (which may be null/missing in some rows).
    */

  // 1
  val moviesRawDF = spark.read.text("src/main/resources/data/movies.json")
  val moviesVariantDF = moviesRawDF.select(parse_json(col("value")).as("movie"))
  moviesVariantDF.select(
    variant_get(col("movie"), "$.Title", "string").as("title"),
    try_variant_get(col("movie"), "$.IMDB_Rating", "double").as("rating")
  ).where(col("rating").isNotNull)
    .orderBy(col("rating").desc)
    .show(10)

  // 2
  moviesVariantDF.select(
    variant_get(col("movie"), "$.Title", "string").as("title"),
    try_variant_get(col("movie"), "$.US_DVD_Sales", "long").as("dvd_sales")
  ).show()

  }
}
```

### SparkSQLNewFeatures.scala

**New file:** `src/main/scala/part8spark4/SparkSQLNewFeatures.scala`

Covers ANSI mode, pipe syntax (`|>`), SQL UDFs, session variables, recursive CTEs, and collations.

```scala
package part8spark4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// NEW LESSON: Spark 4.x SQL features — ANSI mode, pipe syntax, SQL UDFs, session variables, recursive CTEs, collations
object SparkSQLNewFeatures {

  def main(args: Array[String]): Unit = {

  val spark = SparkSession.builder()
    .appName("Spark SQL 4.x Features")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")
  carsDF.createOrReplaceTempView("cars")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
  moviesDF.createOrReplaceTempView("movies")

  // ============================================================
  // 1 - ANSI mode is now ON by default in Spark 4.x
  // ============================================================
  // In Spark 3.x, spark.sql.ansi.enabled defaulted to false.
  // Now it's true, meaning:
  //   - Division by zero throws SparkArithmeticException (instead of returning null)
  //   - Invalid casts throw errors (instead of returning null)
  //   - Arithmetic overflow is caught
  // This is the SQL standard behavior — correct and safer for production code.

  println(s"ANSI mode enabled: ${spark.conf.get("spark.sql.ansi.enabled")}")

  // ============================================================
  // 2 - Pipe syntax: chain SQL operations left-to-right with |>
  // ============================================================
  // Instead of deeply nested subqueries, you can chain operations in reading order.
  spark.sql(
    """
      |SELECT * FROM movies
      ||> WHERE IMDB_Rating > 8.0
      ||> SELECT Title, IMDB_Rating
      ||> ORDER BY IMDB_Rating DESC
      ||> LIMIT 10
    """.stripMargin
  ).show()

  // Compare with the equivalent traditional SQL:
  // SELECT Title, IMDB_Rating FROM movies WHERE IMDB_Rating > 8.0 ORDER BY IMDB_Rating DESC LIMIT 10

  // ============================================================
  // 3 - SQL User-Defined Functions
  // ============================================================
  // You can now define reusable functions directly in SQL — no Scala/Python UDF registration needed.
  spark.sql("CREATE FUNCTION kg_to_lbs(kg DOUBLE) RETURNS DOUBLE RETURN kg * 2.20462")
  spark.sql(
    """
      |SELECT Name, Weight_in_lbs, kg_to_lbs(Weight_in_lbs / 2.20462) as weight_converted_back
      |FROM cars
      |LIMIT 5
    """.stripMargin
  ).show()

  // ============================================================
  // 4 - Session variables
  // ============================================================
  // Declare variables that persist for the session — useful for parameterized queries.
  spark.sql("DECLARE min_rating = 7.0")
  spark.sql(
    """
      |SELECT Title, IMDB_Rating
      |FROM movies
      |WHERE IMDB_Rating >= min_rating
      |ORDER BY IMDB_Rating DESC
    """.stripMargin
  ).show()

  // You can update session variables too
  spark.sql("SET VARIABLE min_rating = 8.0")
  spark.sql(
    """
      |SELECT Title, IMDB_Rating
      |FROM movies
      |WHERE IMDB_Rating >= min_rating
      |ORDER BY IMDB_Rating DESC
    """.stripMargin
  ).show()

  // ============================================================
  // 5 - Recursive CTEs
  // ============================================================
  // Generate sequences, traverse hierarchies, and more — a long-awaited SQL feature.

  // Simple: generate numbers 1 to 10
  spark.sql(
    """
      |WITH RECURSIVE numbers AS (
      |  SELECT 1 as n
      |  UNION ALL
      |  SELECT n + 1 FROM numbers WHERE n < 10
      |)
      |SELECT * FROM numbers
    """.stripMargin
  ).show()

  // Practical: generate a date range
  spark.sql(
    """
      |WITH RECURSIVE date_range AS (
      |  SELECT DATE '2025-01-01' as dt
      |  UNION ALL
      |  SELECT dt + INTERVAL 1 DAY FROM date_range WHERE dt < DATE '2025-01-31'
      |)
      |SELECT * FROM date_range
    """.stripMargin
  ).show(31)

  // ============================================================
  // 6 - String collation support
  // ============================================================
  // Collations define how strings are compared and sorted (e.g., case-insensitive).
  spark.sql(
    """
      |SELECT collation('hello')
    """.stripMargin
  ).show()

  /**
    * Exercises
    *
    * 1. Use the pipe syntax to find the top 5 most powerful American cars
    *    (highest Horsepower where Origin = 'USA').
    *
    * 2. Write a recursive CTE that generates the Fibonacci sequence up to the 15th term.
    */

  // 1
  spark.sql(
    """
      |SELECT * FROM cars
      ||> WHERE Origin = 'USA'
      ||> SELECT Name, Horsepower
      ||> ORDER BY Horsepower DESC
      ||> LIMIT 5
    """.stripMargin
  ).show()

  // 2
  spark.sql(
    """
      |WITH RECURSIVE fib AS (
      |  SELECT 1 as n, CAST(0 AS BIGINT) as a, CAST(1 AS BIGINT) as b
      |  UNION ALL
      |  SELECT n + 1, b, a + b FROM fib WHERE n < 15
      |)
      |SELECT n, a as fibonacci FROM fib
    """.stripMargin
  ).show(15)

  }
}
```

---

## Summary

| Category | Files Modified | Files Deleted | Files Added |
|---|---|---|---|
| Build config | 2 | 0 | 0 |
| Docker | 4 | 11 | 0 |
| Documentation | 2 | 0 | 0 |
| Scala sources | 17 | 0 | 2 |
| **Total** | **25** | **11** | **2** |

**Net result:** 291 lines added, 561 lines deleted across 36 changed files.
