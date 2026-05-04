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
