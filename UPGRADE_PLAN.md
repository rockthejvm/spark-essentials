# Spark Essentials Course — Upgrade Plan: Spark 3.5 → 4.1.1

## Overview

This plan covers upgrading the [Rock the JVM Spark Essentials](https://rockthejvm.com/courses/apache-spark-essentials-with-scala) repository and course content from Apache Spark 3.5.0 to **Spark 4.1.1**. The goals are:

1. Improve the Docker setup for reliability
2. Add Windows setup instructions
3. Add new lessons for essential features introduced since Spark 3.5
4. Flag and fix outdated or incorrect code
5. Leave working, accurate lessons untouched

---

## Part 1 — Build & Infrastructure

### 1.1 Update `build.sbt`

**Current state:**
```scala
scalaVersion := "2.13.12"
val sparkVersion = "3.5.0"
val postgresVersion = "42.6.0"
val log4jVersion = "2.20.0"
```

**Updated:**
```scala
name := "spark-essentials"
version := "0.3"
scalaVersion := "2.13.16"

val sparkVersion = "4.1.1"
val postgresVersion = "42.7.4"
val log4jVersion = "2.24.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  "org.apache.logging.log4j" % "log4j-api"  % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
  "org.postgresql" % "postgresql" % postgresVersion
)
```

**Changes explained:**
- Scala bumped to 2.13.16 (latest 2.13.x patch). Spark 4.x dropped Scala 2.12 support; 2.13 is the only supported version.
- Spark 4.1.1 (latest stable).
- PostgreSQL driver and Log4j bumped to latest stable versions.
- **Remove the `resolvers` block entirely.** `bintray-spark-packages` has been dead since Bintray shut down in 2021. The `Typesafe Simple Repository` and `MavenRepository` resolvers are unnecessary — Maven Central (included by default) is sufficient.

### 1.2 Update `project/build.properties`

```properties
sbt.version = 1.10.7
```

Bump from 1.9.6 to 1.10.7 (latest stable 1.10.x) for better Java 17/21 compatibility.

### 1.3 Java Version Requirement

Spark 4.x **requires Java 17 or 21**. Java 8 and 11 are no longer supported. This must be documented in the README. The course should recommend **Eclipse Temurin JDK 21**.

---

## Part 2 — Docker Setup Improvements

### 2.1 Replace Custom Docker Images with Official Apache Spark Images

The current setup builds custom Docker images from scratch (downloading Scala, sbt, Spark into a base image). This is fragile — URLs break, versions drift, and the `dos2unix` workaround for Windows line endings is a recurring pain point.

**Apache Spark now publishes official Docker images** at `apache/spark` on Docker Hub.

**Replace the entire `spark-cluster/` directory** with a single `docker-compose.yml` that uses the official images:

```yaml
services:
  postgres:
    image: postgres:17
    container_name: postgres
    environment:
      TZ: "Europe/Amsterdam"
      POSTGRES_USER: "docker"
      POSTGRES_PASSWORD: "docker"
    ports:
      - "5432:5432"
    volumes:
      - "./sql:/docker-entrypoint-initdb.d"

  spark-master:
    image: apache/spark:4.1.1-scala
    container_name: spark-master
    command: /opt/spark/sbin/start-master.sh
    environment:
      SPARK_MASTER_HOST: spark-master
      SPARK_NO_DAEMONIZE: "true"
    ports:
      - "7077:7077"
      - "9090:8080"
      - "4040:4040"
    volumes:
      - ./spark-cluster/apps:/opt/spark-apps
      - ./spark-cluster/data:/opt/spark-data

  spark-worker:
    image: apache/spark:4.1.1-scala
    command: /opt/spark/sbin/start-worker.sh spark://spark-master:7077
    depends_on:
      - spark-master
    environment:
      SPARK_WORKER_CORES: 1
      SPARK_WORKER_MEMORY: 1G
      SPARK_NO_DAEMONIZE: "true"
    volumes:
      - ./spark-cluster/apps:/opt/spark-apps
      - ./spark-cluster/data:/opt/spark-data
```

**Benefits:**
- No custom Dockerfiles to maintain
- No `build-images.sh`/`.bat` step needed — images are pulled from Docker Hub
- No `dos2unix` issues (no custom shell scripts copied into containers)
- Students run a single `docker compose up --scale spark-worker=3`
- Deterministic versioning — the image tag pins the exact Spark version
- PostgreSQL pinned to version 17 instead of `latest` (prevents surprise breaking changes)

**Files to delete:**
- `spark-cluster/docker/` (entire directory — base, master, worker, submit Dockerfiles)
- `spark-cluster/build-images.sh`
- `spark-cluster/build-images.bat`
- `spark-cluster/env/spark-worker.sh`
- `spark-cluster/docker-compose.yml` (replaced by the unified root-level compose file)
- Root-level `docker-compose.yml` (merged into the new unified file above)

**Files to keep (move to `spark-cluster/`):**
- `spark-cluster/apps/` — for deploying JARs
- `spark-cluster/data/` — for data files used in cluster mode

### 2.2 Simplify Root-Level Helper Scripts

The `docker-clean.sh` / `docker-clean.bat` scripts run `docker rm -f $(docker ps -aq)` which kills **all** containers on the machine, not just the course containers. Replace with:

```bash
#!/bin/bash
docker compose down -v
```

### 2.3 Update `spark-cluster/README.md`

Rewrite to reflect the simplified setup:
1. Install Docker
2. Run `docker compose up --scale spark-worker=3`
3. Access Spark Master UI at `http://localhost:9090`
4. Connect to the master for spark-submit: `docker exec -it spark-master bash`

---

## Part 3 — Windows Setup Instructions

### 3.1 Update `HadoopWindowsUserSetup.md`

The current guide is thorough but has several issues:

**Problems:**
- References JDK 8 download link but says "JDK 11 or higher" — contradictory. With Spark 4.x, JDK 17+ is required.
- References `winutils` from `cdarlint/winutils` repo with `hadoop-3.3.5` — this needs to match Hadoop 3.4.x bundled with Spark 4.1.1.
- Steps 5-6 (full Hadoop configuration) are unnecessary for Spark local mode and cause confusion.

**Updated guide should cover:**

1. **Install JDK 21** (Eclipse Temurin recommended — `https://adoptium.net/`)
2. **Install Docker Desktop for Windows** (WSL 2 backend)
3. **Set `JAVA_HOME`** and **`HADOOP_HOME`** environment variables
4. **Download `winutils.exe` and `hadoop.dll`** for Hadoop 3.4.x from the maintained `kontext-tech/winutils` repository, place in `C:\hadoop\bin`
5. **Set `HADOOP_HOME=C:\hadoop`** and add `%HADOOP_HOME%\bin` to PATH
6. **Note**: Steps 5-6 of the old guide (HDFS configuration, starting Hadoop daemons) are NOT needed for this course and can be skipped entirely.

**Also add**: A note that on Windows, the recommended approach is to use Docker Desktop with WSL 2, which avoids all Hadoop/winutils issues entirely.

### 3.2 Update README.md

Add a clear "Prerequisites" section:
- **Java**: JDK 17 or 21 (recommend Eclipse Temurin 21)
- **Docker**: Docker Desktop (Mac/Windows) or Docker Engine (Linux)
- **IDE**: IntelliJ IDEA with Scala plugin
- **Windows-specific**: See `HadoopWindowsUserSetup.md` or use WSL 2

Remove the separate `build-images.sh`/`.bat` instructions since we're using official images.

---

## Part 4 — Code Review: Lessons to Update

### 4.1 ALL LESSONS — `extends App` Deprecation

**Every lesson** uses `object Foo extends App { ... }`. The `App` trait is **deprecated in Scala 2.13** (since 2.13.7) and was removed in Scala 3. While it still compiles under 2.13.16, it should be replaced with proper `main` methods for forward compatibility and to avoid subtle issues with initialization order.

**Pattern to apply across all files:**

```scala
// Before
object DataFramesBasics extends App {
  val spark = SparkSession.builder() ...
  // body
}

// After
object DataFramesBasics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder() ...
    // body
  }
}
```

**Affected files (all 15 lesson files):**
- `part1recap/ScalaRecap.scala`
- `part2dataframes/DataFramesBasics.scala`
- `part2dataframes/DataSources.scala`
- `part2dataframes/ColumnsAndExpressions.scala`
- `part2dataframes/Aggregations.scala`
- `part2dataframes/Joins.scala`
- `part3typesdatasets/CommonTypes.scala`
- `part3typesdatasets/ComplexTypes.scala`
- `part3typesdatasets/Datasets.scala`
- `part3typesdatasets/ManagingNulls.scala`
- `part4sql/SparkSql.scala`
- `part5lowlevel/RDDs.scala`
- `part6practical/SparkJobAnatomy.scala`
- `part7bigdata/TaxiApplication.scala`
- `playground/Playground.scala`

Note: `part4sql/SparkShell.scala` is just comments — no change needed. `part6practical/TestDeployApp.scala` and `part7bigdata/TaxiEconomicImpact.scala` already use `def main(args: Array[String])` — no change needed.

### 4.2 `ColumnsAndExpressions.scala` — Deprecated Symbol Syntax

**Line 29**: `'Year` uses Scala Symbol literal syntax (`'foo`). This was deprecated in Scala 2.13.0 and is removed in Scala 3.

```scala
// Before (line 29)
'Year, // Scala Symbol, auto-converted to column

// After
Symbol("Year"), // Scala Symbol, auto-converted to column
```

However, a better approach for the course is to simply use `$"Year"` or `col("Year")` instead and mention that the Symbol syntax is no longer recommended. The section shows "various select methods" — keep the variety but replace Symbol with another valid approach and add a comment noting it was removed.

```scala
carsDF.select(
  carsDF.col("Name"),
  col("Acceleration"),
  column("Weight_in_lbs"),
  $"Year", // interpolated string, returns a Column object
  $"Horsepower", // same as above
  expr("Origin") // EXPRESSION
)
```

### 4.3 `SparkSql.scala` — Spark 2.4 Legacy Comment

**Line 14**: Remove the Spark 2.4 comment:
```scala
// only for Spark 2.4 users:
// .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
```
This is no longer relevant and will confuse students.

### 4.4 `SparkJobAnatomy.scala` — Outdated Container Names

**Line 29**: The Docker container name format has changed:
```scala
// Before
// docker exec -it spark-cluster_spark-master_1 bash

// After (with official images and docker compose v2)
// docker exec -it spark-master bash
```

### 4.5 `SparkShell.scala` — Outdated Container Names

Same issue — **line 21** references `docker-spark-cluster_spark-master_1`. Update to `spark-master`.

Also update **line 24**: The Spark binary path in the official Docker image is `/opt/spark/bin/spark-sql` (not `/spark/bin/spark-sql`).

### 4.6 `TestDeployApp.scala` — Outdated Deploy Instructions

**Lines 47-77**: The deployment instructions reference:
- `spark-cluster_spark-master_1` (old container name)
- `/spark/bin/spark-submit` (old path; official image uses `/opt/spark/bin/spark-submit`)
- Building a JAR via IntelliJ artifacts (should also mention sbt assembly as an option)

Updated instructions:
```scala
/**
 * How to run on the Docker cluster:
 *
 * 1. Build the JAR: sbt package (or use IntelliJ Build Artifacts)
 * 2. Copy the JAR to spark-cluster/apps/
 * 3. Start the cluster: docker compose up --scale spark-worker=3
 * 4. Connect: docker exec -it spark-master bash
 * 5. Submit:
 *    /opt/spark/bin/spark-submit \
 *      --class part6practical.TestDeployApp \
 *      --master spark://spark-master:7077 \
 *      --deploy-mode client \
 *      --verbose \
 *      /opt/spark-apps/spark-essentials.jar /opt/spark-data/movies.json /opt/spark-data/goodMovies
 */
```

### 4.7 `Joins.scala` — Join Type Names

**Lines 31-34**: The join type strings `"left_outer"` and `"right_outer"` still work in Spark 4.x, but the preferred names are now `"left"` and `"right"` (which have been supported since Spark 2.x). Consider updating for clarity:

```scala
// left outer join
guitaristsDF.join(bandsDF, joinCondition, "left")

// right outer join
guitaristsDF.join(bandsDF, joinCondition, "right")

// full outer join
guitaristsDF.join(bandsDF, joinCondition, "full")
```

This is a style improvement, not a breaking change — both forms work.

### 4.8 ANSI Mode Awareness

Spark 4.x enables `spark.sql.ansi.enabled = true` by default. This changes behavior in several lessons:

- **Division by zero** now throws `SparkArithmeticException` instead of returning `null`
- **Invalid casts** now throw errors instead of returning `null`
- **Overflow** in arithmetic throws errors

This affects:
- `Aggregations.scala`: No direct impact (no division), but students should know about it.
- `CommonTypes.scala`: The math operations are safe (no division by zero risk in the data).
- `ComplexTypes.scala`: No impact.

**Recommendation**: Add a note in the `DataFramesBasics.scala` lesson (or a new config section) explaining ANSI mode:

```scala
// Spark 4.x enables ANSI mode by default (spark.sql.ansi.enabled = true)
// This means stricter SQL behavior: division by zero throws an error (instead of null),
// invalid casts throw errors, and arithmetic overflow is caught.
// This is the correct, standards-compliant behavior.
```

No code changes needed since the existing examples don't trigger ANSI violations, but the concept should be taught.

---

## Part 5 — Lessons That Are 100% Fine (No Changes Needed)

These lessons are conceptually accurate and code-compatible with Spark 4.1.1 (after the universal `extends App` fix from §4.1):

| File | Reason |
|---|---|
| `part2dataframes/Aggregations.scala` | All aggregation APIs unchanged |
| `part2dataframes/DataSources.scala` | Read/write APIs unchanged; JDBC works the same |
| `part3typesdatasets/CommonTypes.scala` | `lit`, boolean filters, string functions all unchanged |
| `part3typesdatasets/ComplexTypes.scala` | Dates, structs, arrays APIs unchanged |
| `part3typesdatasets/Datasets.scala` | Dataset/Encoder APIs unchanged |
| `part3typesdatasets/ManagingNulls.scala` | `coalesce`, `na.fill`, `na.drop` unchanged |
| `part5lowlevel/RDDs.scala` | RDD API unchanged |
| `part7bigdata/TaxiApplication.scala` | All DataFrame operations unchanged |
| `part7bigdata/TaxiEconomicImpact.scala` | All DataFrame operations unchanged |

---

## Part 6 — New Lessons to Add

### 6.1 New Lesson: `part8spark4/VariantType.scala`

**Topic**: The VARIANT data type — Spark 4's native semi-structured data support.

**Why it's essential**: VARIANT is one of the headline features of Spark 4.0. It provides a native, optimized binary format for JSON-like data that is dramatically faster than storing JSON as strings. Any modern Spark course must cover it.

**Content outline:**
1. What is VARIANT and why it exists (vs. storing JSON as StringType)
2. Creating VARIANT columns with `parse_json()`
3. Querying VARIANT data with `variant_get()` and `try_variant_get()`
4. Schema discovery with `schema_of_variant()`
5. Checking for nulls with `is_variant_null()`
6. VARIANT in SQL vs. DataFrame API

**Code:**
```scala
package part8spark4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object VariantType {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Variant Type")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

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
    val extractedDF = variantDF.select(
      variant_get(col("data"), "$.name", "string").as("name"),
      variant_get(col("data"), "$.age", "int").as("age"),
      try_variant_get(col("data"), "$.address.city", "string").as("city") // returns null if path missing
    )
    extractedDF.show()

    // 3 - Schema discovery
    variantDF.select(schema_of_variant(col("data"))).show(truncate = false)

    // 4 - Checking for VARIANT nulls
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
     * 1. Parse the movies.json file into a single VARIANT column, then extract Title and IMDB_Rating.
     * 2. Use try_variant_get to safely extract a field that may not exist in all rows.
     */

    // 1
    val moviesRawDF = spark.read.text("src/main/resources/data/movies.json")
    val moviesVariantDF = moviesRawDF.select(parse_json(col("value")).as("movie"))
    moviesVariantDF.select(
      variant_get(col("movie"), "$.Title", "string").as("title"),
      try_variant_get(col("movie"), "$.IMDB_Rating", "double").as("rating")
    ).where(col("rating").isNotNull)
      .orderBy(col("rating").desc)
      .show()

    // 2
    moviesVariantDF.select(
      variant_get(col("movie"), "$.Title", "string").as("title"),
      try_variant_get(col("movie"), "$.US_DVD_Sales", "long").as("dvd_sales")
    ).show()
  }
}
```

### 6.2 New Lesson: `part8spark4/SparkSQLNewFeatures.scala`

**Topic**: New SQL features in Spark 4.x — pipe syntax, SQL UDFs, session variables, recursive CTEs.

**Why it's essential**: These features bring Spark SQL much closer to full SQL feature parity and improve readability. Recursive CTEs are especially important for hierarchical data.

**Content outline:**
1. ANSI mode (now default) and what it means
2. SQL Pipe syntax (`|>`)
3. SQL User-Defined Functions (`CREATE FUNCTION`)
4. Session variables (`DECLARE`)
5. Recursive CTEs
6. Collation support

**Code:**
```scala
package part8spark4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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

    // 1 - ANSI mode is now ON by default in Spark 4.x
    // Division by zero now throws an error instead of returning null
    // Invalid casts now throw errors instead of returning null
    // This is the SQL standard behavior

    // 2 - Pipe syntax: chain SQL operations left-to-right
    spark.sql(
      """
        |SELECT * FROM movies
        ||> WHERE IMDB_Rating > 8.0
        ||> SELECT Title, IMDB_Rating
        ||> ORDER BY IMDB_Rating DESC
        ||> LIMIT 10
      """.stripMargin
    ).show()

    // 3 - SQL User-Defined Functions
    spark.sql("CREATE FUNCTION kg_to_lbs(kg DOUBLE) RETURNS DOUBLE RETURN kg * 2.20462")
    spark.sql(
      """
        |SELECT Name, Weight_in_lbs, kg_to_lbs(Weight_in_lbs / 2.20462) as weight_converted_back
        |FROM cars
        |LIMIT 5
      """.stripMargin
    ).show()

    // 4 - Session variables
    spark.sql("DECLARE min_rating = 7.0")
    spark.sql(
      """
        |SELECT Title, IMDB_Rating
        |FROM movies
        |WHERE IMDB_Rating >= min_rating
        |ORDER BY IMDB_Rating DESC
      """.stripMargin
    ).show()

    // 5 - Recursive CTEs (great for hierarchical data)
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

    // A more practical recursive CTE: generating a date range
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

    // 6 - String collation support
    spark.sql(
      """
        |SELECT collation('hello')
      """.stripMargin
    ).show()

    /**
     * Exercises
     *
     * 1. Use the pipe syntax to find the top 5 most powerful American cars (highest Horsepower, Origin = 'USA').
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
        |  SELECT 1 as n, 0L as a, 1L as b
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

## Part 7 — Summary of All Changes

### Files to Modify

| File | Changes |
|---|---|
| `build.sbt` | Spark 4.1.1, Scala 2.13.16, remove dead resolvers, bump deps |
| `project/build.properties` | sbt 1.10.7 |
| `docker-compose.yml` | Unified compose file with official Spark + PostgreSQL images |
| `README.md` | Rewrite setup instructions, add prerequisites section |
| `HadoopWindowsUserSetup.md` | Update for JDK 21, Hadoop 3.4.x winutils, add WSL 2 note |
| `docker-clean.sh` / `docker-clean.bat` | Use `docker compose down -v` |
| All 15 lesson files | Replace `extends App` with `def main(args: Array[String])` |
| `part2dataframes/ColumnsAndExpressions.scala` | Replace `'Year` Symbol syntax |
| `part4sql/SparkSql.scala` | Remove Spark 2.4 legacy comment |
| `part4sql/SparkShell.scala` | Update container names and Spark paths |
| `part6practical/SparkJobAnatomy.scala` | Update container names |
| `part6practical/TestDeployApp.scala` | Update deploy instructions, container names, Spark paths |

### Files to Add

| File | Purpose |
|---|---|
| `src/main/scala/part8spark4/VariantType.scala` | New lesson: VARIANT data type |
| `src/main/scala/part8spark4/SparkSQLNewFeatures.scala` | New lesson: Pipe syntax, SQL UDFs, session variables, recursive CTEs, ANSI mode, collations |

### Files to Delete

| File | Reason |
|---|---|
| `spark-cluster/docker/` (entire directory) | Replaced by official Docker images |
| `spark-cluster/build-images.sh` | No longer needed |
| `spark-cluster/build-images.bat` | No longer needed |
| `spark-cluster/env/spark-worker.sh` | No longer needed |
| `spark-cluster/docker-compose.yml` | Merged into root-level compose file |

### Files That Need No Changes

| File | Reason |
|---|---|
| `part1recap/ScalaRecap.scala` | Pure Scala recap, all concepts valid (only needs `extends App` fix) |
| `part2dataframes/DataFramesBasics.scala` | All APIs unchanged (only needs `extends App` fix) |
| `part2dataframes/DataSources.scala` | Read/write APIs unchanged |
| `part2dataframes/Aggregations.scala` | Aggregation APIs unchanged |
| `part2dataframes/Joins.scala` | Join APIs unchanged (optional: use shorter join type names) |
| `part3typesdatasets/CommonTypes.scala` | All type APIs unchanged |
| `part3typesdatasets/ComplexTypes.scala` | Date/struct/array APIs unchanged |
| `part3typesdatasets/Datasets.scala` | Dataset/Encoder APIs unchanged |
| `part3typesdatasets/ManagingNulls.scala` | Null handling APIs unchanged |
| `part5lowlevel/RDDs.scala` | RDD API unchanged |
| `part7bigdata/TaxiApplication.scala` | All operations unchanged |
| `part7bigdata/TaxiEconomicImpact.scala` | All operations unchanged |
| `playground/Playground.scala` | Basic playground, APIs unchanged |
| `sql/db.sql` | PostgreSQL init script, no Spark dependency |
| `psql.sh` / `psql.bat` | Simple Docker exec scripts |
| `src/main/resources/data/*` | Data files, format-independent |
| `src/main/resources/log4j2.properties` | Log4j config, works with updated version |

---

## Part 8 — Course Content Recommendations

### 8.1 Mention in Existing Lectures

- **DataFramesBasics**: Mention that Spark 4.x requires Java 17+ and ANSI mode is now on by default.
- **SparkSql**: Mention that `CREATE TABLE` without `USING` now uses the default data source (Parquet) instead of creating a Hive table.
- **Joins**: Optionally mention that `"left"`, `"right"`, and `"full"` are the preferred join type strings (the `_outer` suffix versions still work).

### 8.2 Topics Intentionally NOT Added

These Spark 4.x features are real but not "essentials"-level:

- **Spark Connect** (client-server architecture) — advanced deployment topic
- **Declarative Pipelines** — new in 4.1 but a separate paradigm
- **SQL Scripting** (procedural SQL with loops/conditionals) — too advanced for essentials
- **Python Data Source API** — Scala course
- **Structured Streaming** — deserves its own course

### 8.3 Updated Course Section Structure

```
Part 1: Scala Recap
Part 2: DataFrames
Part 3: Types & Datasets
Part 4: Spark SQL
Part 5: Low-Level APIs (RDDs)
Part 6: Practical (Job Anatomy, Deployment)
Part 7: Big Data Project (Taxi)
Part 8: Spark 4.x Features    ← NEW
  - Variant Type
  - SQL New Features (Pipe Syntax, UDFs, Recursive CTEs, ANSI Mode)
```
