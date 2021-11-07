package mikeyVersion

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object DataSources extends App {

  //Create a spark app for an entry point

  val resourcesPathF = (s: String) => s"src/main/resources/data/$s"

  val spark: SparkSession =
    SparkSession.builder()
      .appName("DataFrames Basics")
      .config("spark.master", "local") //local because running on our machine, in prod obviously not
      .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val carsSchemaDateType = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType), //dates are sensitive so be careful - coupled with schema if spark fails parsing it will put null
    StructField("Origin", StringType)
  ))

  /*
  * Reading a df:
  * -format
  * - schema (optional)
  *
  * */

  val firstDF: DataFrame =
    spark.read
      .format("json")
      .schema(carsSchema) // enforce a schema
      //      .option("inferSchema", "true")
      .option("mode", "failFast") // some options include dropMalformed, permissive (default), failfast crashes eargerly when parsing invalid values
      .load(resourcesPathF("cars.json"))

  val firstDF2: DataFrame =
    spark.read
      .format("json")
      .schema(carsSchema) // enforce a schema
      .option("mode", "failFast") // some options include dropMalformed, permissive (default)
      .option("path", resourcesPathF("cars.json")) // some options include dropMalformed, permissive (default))
      .load()

  val carsDFOptionsMap: DataFrame =
    spark.read
      .format("json")
      .options(Map(
        "mode" -> "failFast",
        "path" -> resourcesPathF("cars.json"),
        "inferSchema" -> "true",
      ))
      .load()

  // writing DFs

  val writeDFToJson =
    firstDF.write
      .format("json")
      .mode(SaveMode.Overwrite) // more typesafe
      //    .option("path", pathPrefixF("cars_dupe.json")  //alternative)
      .save(resourcesPathF("cars_dupe.json"))

  // JSON flags

  val jsonFlags =
    spark.read
      .schema(carsSchemaDateType)
      .option("dateFormat", "YYYY-MM-dd")
      .option("allowSingleQuotes", "true")
      .option("compression", "uncompressed") //bzip, gzip, lz4, snappy, deflate
      .json(resourcesPathF("cars.json")) // alternative to  .format("json"))

  // CSV flags - big pain

  val stocksSchema =
    StructType(Array(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    ))

  val readStocks =
    spark.read
      .format("csv")
      .schema(stocksSchema)
      .option("dateFormat", "MMM dd YYYY")
      .option("header", "true") //ignore the first row
      .option("sep", ",") //split by commas
      .option("nullValue", "") // instruct spark to parse empty string as null since in csv no concept of null
      .csv(resourcesPathF("stocks.csv"))

  // Parquet  - pronounced par-K  compressed binary data format, parquet is the default storage for spark, very predictable data store format

  val parquets =
    firstDF.write
      .mode(SaveMode.Overwrite)
      //      .parquet(resourcesPathF("cars.parquet"))  //parquet is default so redundant can use .save()  more compression in snappy parquet vs json
      .save(resourcesPathF("cars.parquet"))

  // Text files
  val readText =
    spark.read.text(resourcesPathF("sampleTextFile.txt"))

  readText.show()

  val driver = "org.postgresgl.Driver"
  val url = "jdbc:postgresgl://localhost:5432/rtjvm"
  val username = "docker"
  val password = "docker"

  val readingFromRemoteDb =
    spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", username)
      .option("password", password)
      .option("dbtable", "public.employees")
      .load()

  val moviesDF =
    spark.read
      .json(resourcesPathF("movies.json"))

  val writeMoviesCSV =
    moviesDF.write
      .format("csv")
      .option("header", "true")
      .option("sep", "\t") // separate via tab
      .save(resourcesPathF("movies.csv"))

  val writeMoviesParquet =
    moviesDF.write
      .save(resourcesPathF("movies.parquet"))

  val writeMoviesRemoteDb =
    moviesDF.write
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", username)
      .option("password", password)
      .option("dbtable", "public.movies")
      .save()
}
