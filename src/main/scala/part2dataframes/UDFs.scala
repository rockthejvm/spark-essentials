package part2dataframes

import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.functions._

object UDFs {

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // 1 - create a Scala function
  val countWords = (text: String) =>
    text.split("\\s+").length

  // 2 - register the UDF
  val countWordsUDF = udf(countWords)

  // 3 - use the UDF as a normal Spark function
  val moviesWithWordCountDF = moviesDF.select(
    col("Title"),
    countWordsUDF(col("Title")).as("Title_Words")
  )

  // UDFs with multiple arguments
  val carsDF = spark.read.json("src/main/resources/data/cars.json")

  // 1
  val carCategoryFn = (weight: Long, cylinders: Long) =>
    if (weight >= 4000 || cylinders >= 8) "Heavy"
    else if (weight >= 3000 || cylinders >= 6) "Medium"
    else "Light"

  // 2
  val carCategoryUDF = udf(carCategoryFn)

  // 3
  val carsWithCategoryDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    col("Cylinders"),
    carCategoryUDF(col("Weight_in_lbs"),col("Cylinders")).as("Category")
  )

  /**
    * Exercise - register a UDF that takes the brand of the cars from their name (first word), capitalize it.
    * Then show all the distinct brands ordered alphabetically.
    */

  // 1
  val extractBrandFn = (name: String) =>
    name.split(" ")(0).capitalize

  // 2
  val extractBrandUDF = udf(extractBrandFn)

  // 3
  val brandsDF = carsDF.select(extractBrandUDF(col("Name")).alias("Brand"))
    .distinct()
    .orderBy("Brand")

  def main(args: Array[String]): Unit = {
    brandsDF.show()
  }
}
