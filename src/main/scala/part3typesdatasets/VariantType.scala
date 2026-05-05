package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object VariantType {

  def main(args: Array[String]): Unit = {

  val spark = SparkSession.builder()
    .appName("Variant Type")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  // The VARIANT type stores JSON-like data in an optimized binary format —
  // much faster than storing JSON as strings, and no predefined schema required.

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
