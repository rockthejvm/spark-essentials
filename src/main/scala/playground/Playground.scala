package playground

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * This is a small application that loads some manually inserted rows into a Spark DataFrame.
  * Feel free to modify this code as you see fit, fiddle with the code and play with your own exercises, ideas and datasets.
  *
  * Daniel @ Rock the JVM
  */
object Playground extends App {

  /**
    * This creates a SparkSession, which will be used to operate on the DataFrames that we create.
    */
  val spark = SparkSession.builder()
    .appName("Spark Essentials Playground App")
    .config("spark.master", "local")
    .getOrCreate()

  /**
    * The SparkContext (usually denoted `sc` in code) is the entry point for low-level Spark APIs, including access to Resilient Distributed Datasets (RDDs).
    */
  val sc = spark.sparkContext

  /**
    * A Spark schema structure that describes a small cars DataFrame.
    */
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", LongType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  /**
    * A "manual" sequence of rows describing cars, fetched from cars.json in the data folder.
    */
  val cars = Seq(
    Row("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA"),
    Row("buick skylark 320",15,8,350,165,3693,11.5,"1970-01-01","USA"),
    Row("plymouth satellite",18,8,318,150,3436,11.0,"1970-01-01","USA"),
    Row("amc rebel sst",16,8,304,150,3433,12.0,"1970-01-01","USA"),
    Row("ford torino",17,8,302,140,3449,10.5,"1970-01-01","USA"),
    Row("ford galaxie 500",15,8,429,198,4341,10.0,"1970-01-01","USA"),
    Row("chevrolet impala",14,8,454,220,4354,9.0,"1970-01-01","USA"),
    Row("plymouth fury iii",14,8,440,215,4312,8.5,"1970-01-01","USA"),
    Row("pontiac catalina",14,8,455,225,4425,10.0,"1970-01-01","USA"),
    Row("amc ambassador dpl",15,8,390,190,3850,8.5,"1970-01-01","USA")
  )

  /**
    * The two lines below create an RDD of rows (think of an RDD like a parallel collection).
    * Then from the RDD we create a DataFrame, which has a number of useful querying methods.
    */
  val carsRows = sc.parallelize(cars)
  val carsDF = spark.createDataFrame(carsRows, carsSchema)

  /**
    * If the schema and the contents of the DataFrame are printed correctly to the console,
    * this means the libraries work correctly and you can jump into the course!
    */
  carsDF.printSchema()
  carsDF.show()

}
