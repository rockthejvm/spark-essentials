package part3typesdatasets

import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.functions._

object VariantType {

  val spark = SparkSession.builder()
    .master("local")
    .getOrCreate()

  import spark.implicits._

  // simple df
  val jsonDF = List(
    """ {"name":"Alice", "age": 30, "address": {"city":"NYC", "zip":12345} } """,
    """ {"name":"Bob", "age": 25, "skills": ["Scala", "Apache Spark"] } """,
    """ {"name":"Charlie", "age": 40, "address": {"city":"SF"} } """,
  ).toDF("raw_json")

  // parse the strings as variant types
  val variantDF = jsonDF.select(parse_json(col("raw_json")).as("data"))

  // extract data from variant types
  val extractedDF = variantDF.select(
    variant_get(col("data"), "$.name", "string").as("name"),
    variant_get(col("data"), "$.age", "int").as("age"),
    try_variant_get(col("data"), "$.address.city", "string").as("city")
  )

  // explore the schemas of your rows and ignore the data itself
  val schemaDF = variantDF.select(schema_of_variant(col("data")))

  /*
    1 - read the movies DF as TEXT, then parse the JSONs as variants
      then extract Title and IMDB rating, show the top 10 by rating.
    2 - use try_variant_get to safely extract US_DVD_Sales, show them alongside the movie titles
   */

  // 1
  val moviesDF = spark.read.text("src/main/resources/data/movies.json")
    .select(parse_json(col("value")).as("movie"))

  val moviesExtractedDF = moviesDF.select(
    variant_get(col("movie"), "$.Title", "string").as("title"),
    try_variant_get(col("movie"), "$.IMDB_Rating", "string").as("rating")
  ).filter(col("rating").isNotNull)
    .orderBy(col("rating").desc)
    .limit(10)

  // 2
  val dvdSalesDF = moviesDF.select(
    variant_get(col("movie"), "$.Title", "string").as("title"),
    try_variant_get(col("movie"), "$.US_DVD_Sales", "string").as("title"),
  )

  def main(args: Array[String]): Unit = {
    variantDF.printSchema()
    variantDF.show(truncate = false)

    extractedDF.show()
    schemaDF.show(truncate = false)

    moviesExtractedDF.show()
    dvdSalesDF.show()
  }
}
