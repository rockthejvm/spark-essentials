package part3typesdatasets

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._

// NEW LESSON: User-Defined Functions (UDFs) and User-Defined Aggregate Functions (UDAFs)
object UDFsAndUDAFs {

  def main(args: Array[String]): Unit = {

  val spark = SparkSession.builder()
    .appName("UDFs and UDAFs")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // ============================================================
  // Part 1: User-Defined Functions (UDFs)
  // ============================================================
  // UDFs let you use custom Scala functions inside DataFrame operations.
  // Use them when built-in Spark functions don't cover your logic.

  // Define a Scala function
  val countWords = (text: String) => {
    if (text == null) 0
    else text.split("\\s+").length
  }

  // Wrap it as a Spark UDF
  val countWordsUDF = udf(countWords)

  // Use the UDF in a select
  moviesDF.select(
    col("Title"),
    countWordsUDF(col("Title")).as("Title_Words")
  ).show()

  // UDFs with multiple arguments
  val weightCategory = (weight: java.lang.Long, cylinders: java.lang.Long) => {
    if (weight == null || cylinders == null) "Unknown"
    else if (weight > 4000) "Heavy"
    else if (weight > 3000) "Medium"
    else "Light"
  }

  val weightCategoryUDF = udf(weightCategory)
  carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    col("Cylinders"),
    weightCategoryUDF(col("Weight_in_lbs"), col("Cylinders")).as("Category")
  ).show()

  // ============================================================
  // Registering UDFs for SQL
  // ============================================================
  // spark.udf.register makes the UDF available in SQL expressions
  spark.udf.register("count_words", countWords)

  moviesDF.createOrReplaceTempView("movies")
  spark.sql(
    """
      |SELECT Title, count_words(Title) as Title_Words
      |FROM movies
      |ORDER BY Title_Words DESC
      |LIMIT 10
    """.stripMargin
  ).show()

  // You can also register and define in one step
  spark.udf.register("classify_rating", (rating: Double) => {
    if (rating >= 8.0) "Excellent"
    else if (rating >= 6.0) "Good"
    else if (rating >= 4.0) "Average"
    else "Poor"
  })

  spark.sql(
    """
      |SELECT Title, IMDB_Rating, classify_rating(IMDB_Rating) as Quality
      |FROM movies
      |WHERE IMDB_Rating IS NOT NULL
      |ORDER BY IMDB_Rating DESC
      |LIMIT 10
    """.stripMargin
  ).show()

  // ============================================================
  // Part 2: User-Defined Aggregate Functions (UDAFs)
  // ============================================================
  // UDAFs aggregate values across rows, like sum() or avg() but with custom logic.
  // In Spark 4.x, you define UDAFs by extending Aggregator[IN, BUF, OUT].
  // (The old UserDefinedAggregateFunction class was removed in Spark 4.0.)

  // Example: a UDAF that computes the geometric mean of a column
  // Geometric mean = (x1 * x2 * ... * xn) ^ (1/n)
  // We use log sums to avoid overflow: exp(sum(log(x)) / n)

  case class GeometricMeanBuffer(logSum: Double, count: Long)

  object GeometricMeanAggregator extends Aggregator[Double, GeometricMeanBuffer, Double] {
    // initial value of the buffer
    def zero: GeometricMeanBuffer = GeometricMeanBuffer(0.0, 0L)

    // add one input value to the buffer
    def reduce(buffer: GeometricMeanBuffer, input: Double): GeometricMeanBuffer = {
      if (input > 0) GeometricMeanBuffer(buffer.logSum + math.log(input), buffer.count + 1)
      else buffer // skip non-positive values
    }

    // merge two buffers (needed when Spark combines partial results from different partitions)
    def merge(b1: GeometricMeanBuffer, b2: GeometricMeanBuffer): GeometricMeanBuffer =
      GeometricMeanBuffer(b1.logSum + b2.logSum, b1.count + b2.count)

    // produce the final output from the buffer
    def finish(buffer: GeometricMeanBuffer): Double = {
      if (buffer.count == 0) 0.0
      else math.exp(buffer.logSum / buffer.count)
    }

    def bufferEncoder: Encoder[GeometricMeanBuffer] = Encoders.product
    def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  // Use as a column function
  val geometricMean = udaf(GeometricMeanAggregator)

  moviesDF.select(
    avg(col("IMDB_Rating")).as("Arithmetic_Mean"),
    geometricMean(col("IMDB_Rating")).as("Geometric_Mean")
  ).show()

  // Use per group
  moviesDF
    .where(col("Major_Genre").isNotNull && col("IMDB_Rating").isNotNull)
    .groupBy("Major_Genre")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      geometricMean(col("IMDB_Rating")).as("Geometric_Mean_Rating")
    )
    .orderBy(col("Avg_Rating").desc)
    .show()

  // Register for SQL use
  spark.udf.register("geo_mean", udaf(GeometricMeanAggregator))
  spark.sql(
    """
      |SELECT Major_Genre,
      |       AVG(IMDB_Rating) as avg_rating,
      |       geo_mean(IMDB_Rating) as geo_mean_rating
      |FROM movies
      |WHERE Major_Genre IS NOT NULL AND IMDB_Rating IS NOT NULL
      |GROUP BY Major_Genre
      |ORDER BY avg_rating DESC
    """.stripMargin
  ).show()

  // ============================================================
  // Another UDAF example: custom string aggregation (concat all values)
  // ============================================================

  object StringConcatAggregator extends Aggregator[String, String, String] {
    def zero: String = ""
    def reduce(buffer: String, input: String): String = {
      if (buffer.isEmpty) input
      else if (input == null) buffer
      else s"$buffer, $input"
    }
    def merge(b1: String, b2: String): String = {
      if (b1.isEmpty) b2
      else if (b2.isEmpty) b1
      else s"$b1, $b2"
    }
    def finish(buffer: String): String = buffer
    def bufferEncoder: Encoder[String] = Encoders.STRING
    def outputEncoder: Encoder[String] = Encoders.STRING
  }

  val concatAll = udaf(StringConcatAggregator)

  carsDF
    .groupBy("Origin")
    .agg(
      count("*").as("Count"),
      concatAll(col("Name")).as("All_Cars")
    )
    .show(truncate = false)

  /**
    * Exercises
    *
    * 1. Create a UDF that takes a car name and returns the "brand" (first word of the name).
    *    Use it to show the distinct brands in the cars dataset.
    *    Then register it for SQL and write a SQL query that counts cars per brand.
    *
    * 2. Create a UDAF that computes the range (max - min) of a numeric column.
    *    Use it to show the range of IMDB ratings per genre in the movies dataset.
    */

  // 1
  val extractBrand = (name: String) => {
    if (name == null) "Unknown"
    else name.split("\\s+")(0)
  }

  val extractBrandUDF = udf(extractBrand)
  carsDF.select(extractBrandUDF(col("Name")).as("Brand"))
    .distinct()
    .orderBy("Brand")
    .show(50)

  spark.udf.register("extract_brand", extractBrand)
  carsDF.createOrReplaceTempView("cars")
  spark.sql(
    """
      |SELECT extract_brand(Name) as Brand, COUNT(*) as Count
      |FROM cars
      |GROUP BY Brand
      |ORDER BY Count DESC
    """.stripMargin
  ).show()

  // 2
  case class RangeBuffer(min: Double, max: Double, hasValue: Boolean)

  object RangeAggregator extends Aggregator[Double, RangeBuffer, Double] {
    def zero: RangeBuffer = RangeBuffer(Double.MaxValue, Double.MinValue, false)
    def reduce(buffer: RangeBuffer, input: Double): RangeBuffer =
      RangeBuffer(math.min(buffer.min, input), math.max(buffer.max, input), true)
    def merge(b1: RangeBuffer, b2: RangeBuffer): RangeBuffer = {
      if (!b1.hasValue) b2
      else if (!b2.hasValue) b1
      else RangeBuffer(math.min(b1.min, b2.min), math.max(b1.max, b2.max), true)
    }
    def finish(buffer: RangeBuffer): Double =
      if (!buffer.hasValue) 0.0 else buffer.max - buffer.min
    def bufferEncoder: Encoder[RangeBuffer] = Encoders.product
    def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  val rangeUDAF = udaf(RangeAggregator)

  moviesDF
    .where(col("Major_Genre").isNotNull && col("IMDB_Rating").isNotNull)
    .groupBy("Major_Genre")
    .agg(
      min("IMDB_Rating").as("Min_Rating"),
      max("IMDB_Rating").as("Max_Rating"),
      rangeUDAF(col("IMDB_Rating")).as("Rating_Range")
    )
    .orderBy(col("Rating_Range").desc)
    .show()

  }
}
