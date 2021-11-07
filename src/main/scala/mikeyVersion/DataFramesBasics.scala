package mikeyVersion

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object DataFramesBasics extends App {

  //Create a spark app for an entry point

  val spark: SparkSession =
    SparkSession.builder()
      .appName("DataFrames Basics")
      .config("spark.master", "local") //local because running on our machine, in prod obviously not
      .getOrCreate()

  // reading a DF

  val firstDF: DataFrame =
    spark.read
      .format("json") // read in some json data from a file path
      .option("inferSchema", "true") // inferSchema not ideal since spark can get it wrong
      .load("src/main/resources/data/cars.json") // file path goes here for your data


  firstDF.show() // showing a dataframe
  //  firstDF.printSchema() // returns the descriptions of each column in the table generated from the json

  // a dataframe is essentially the schema and the rows conforming the data to that structure

  firstDF.take(10).foreach(println) // prints out the first 10 rows, you can use take to get rows

  // spark types
  // import org.apache.spark.sql.types._

  val longTypes: LongType.type = LongType

  val schemaExample: StructType =
    StructType(
      Array(StructField("Name", StringType, nullable = true))
    )

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

  // obtain a schema
  val carsDFSchema = firstDF.schema

  println(carsDFSchema)

  // read a dataframe with your own schema

  val carsDFWithSchema =
    spark.read
      .format("json")
      .schema(carsDFSchema)
      .load("src/main/resources/data/cars.json")


  // create rows by hand
  //  import org.apache.spark.sql.Row

  val myRow = Row("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA")

  // create df from tuples

  val cars: Seq[(String, Double, Long, Double, Long, Long, Double, String, String)] =
    Seq(
      ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
      ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
      ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
      ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
      ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
      ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
      ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
      ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
      ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
      ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
    )

  val manualCarsDF = spark.createDataFrame(cars) //schema auto-inferred

  // note: DFs have schemas, rows do not

  // create dfs with implicits

  import spark.implicits._

  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "Country")

  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()

  //Rows = unstructured data
  //Schema = description of fields aka columns and their type

  // Exercises

  val phones: Seq[(String, String, Double, Double)] =
    Seq(
      ("iphone", "X", 20.0, 50.0),
      ("android", "S10", 20.0, 50.0),
      ("oneplus", "v1", 20.0, 50.0),
      ("nokia", "v2", 20.0, 50.0),
      ("xiaomei", "v3", 20.0, 50.0),
      ("huawei", "v4", 20.0, 50.0),
      ("google", "v5", 20.0, 50.0),
      ("sony", "v6", 20.0, 50.0)
    )

  val createDF = spark.createDataFrame(phones)

  val phonesToDF = phones.toDF("make", "model", "dimensions", "megapixels")

  //  phonesToDF.printSchema()
  phonesToDF.show()

  // exercise 2 read from data

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.printSchema()
  println(moviesDF.count())

  /*
   dataframes can be thought as distributed spreadsheets with rows and columns
   dataframes are collections or rows conforming to a schema
   schema = list describing the column names and column types  - StructFields
   - types are known to Spark not at compile time
   - arbitrary number of columns
   - all rows have the same structure

   The Dataframes need to be distributed since they are either:
    - data is too big for a single computer
    - data takes too long to process on a single cpu

  Partitioning
    - data split into files and shared between nodes in the spark cluster
    - impacts the processing parallelism of your data
    - 1000 paritions and only 1 node = processing parallism = 1, since only 1 node to process your data
    - 1 paritions and 1,000,000 nodes = processing parallism = 1, since only 1 partition despite the million of nodes available to process it,
      only a single node will have access to the 1 partition
    - partitioning and nodes have to do with performance of spark and your data processing, big topic


   dataframes are immutable
   new dataframes have to be created via transformations
   narrow
   */
}
