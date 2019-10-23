package part7bigdata

import org.apache.spark.sql.SparkSession

object TaxiApplication extends App {

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Taxi Big Data Application")
    .getOrCreate()

  val bigTaxiDF = spark.read.load("/Users/daniel/Downloads/NYC_taxi_2009-2016.parquet")

  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiDF.printSchema()

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")
  taxiZonesDF.printSchema()

  /**
    * Questions:
    *
    * 1. Which zones have the most pickups/dropoffs overall?
    * 2. What are the peak hours for taxi?
    * 3. How are the trips distributed? Why are people taking the cab?
    * 4. What are the peak hours for long/short trips?
    * 5. What are the top 3 pickup/dropoff zones for long/short trips?
    * 6. How are people paying for the ride, on long/short trips?
    * 7. How is the payment type evolving with time?
    * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
    * 
    */
}
