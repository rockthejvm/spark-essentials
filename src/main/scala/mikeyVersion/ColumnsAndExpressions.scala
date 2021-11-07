package mikeyVersion

import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  //Columns

  val firstColumn = carsDF.col("Name")

  //selecting - "projection" is the technical term, we are projecting the dataframe into another dataframe that has less data i.e. column

  val carNamesDF = carsDF.select(firstColumn)

  carNamesDF.show()

  // various select methods

  // Some different ways of calling columns some require imports such as //   import spark.implicits or  import org.apache.spark.sql.functions.{col, column, expr}

  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"), // convenience method
    column("Weight_in_lbs"),
    'Year, // Scala symbol - auto converted into a column
    $"Horsepower", // Fancier string interpolation that returns a column in the context of Spark but watch out it's not an s"" but $"" with a dollar
    expr("Origin") // Expression
  )

  carsDF.select("Name", "Year")

  // select is very powerful and is a narrow transformation
  // the DFs are split into partitions on to different nodes within the cluster of machines, so when you select column you are selecting from every node in the cluster
  // you then return a new DF as the columns you have chose. This is then reflected on every node/machine in the cluster.
  // every narrow transformation in a nutshell every input partition has one corresponding output partition to reflect the transformation in the new resultant DF.

  // Expressions

  val simplestExpression =
    carsDF.col("Weight_in_lbs")

  val weightInKgExpression: Column = carsDF.col("Weight_in_lbs") / 2.205

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight in kg"),
    expr("Weight_in_lbs / 2.205").as("Weight in kg v2"),
  )

  carsWithWeightsDF.show()

  // selectExpression

  val carsWithSelectExprWeightDF = // convenient way of using expressions
    carsDF.selectExpr(
      "Name",
      "Weight_in_lbs",
      "Weight_in_lbs / 2.2"
    )

  // adding a column for weight in kg v3
  val carsWithKgDF3 = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.205)

  //renaming a column

  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")

  //careful with column names whilst using expressions for spaces you need backticks
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")

  // removing a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering

  val euroCarsDF = carsDF.filter(col("Origin") =!= "USA")

  val euroCarsDF2 = carsDF.where(col("Origin") =!= "USA") //same as filter here

  val americanCarsDF = carsDF.filter("Origin = 'USA'") //needs single quotes for 'USA'

  // chaining filters
  // these are all the same
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter((col("Origin") === "USA").and(col("Horsepower") > 150)) // more powerful filter using .and()
  val americanPowerfulCarsDFAnd2 = carsDF.filter((col("Origin") === "USA") and (col("Horsepower") > 150)) // infix and
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150") // expression string version

  // Adding more rows - Unioning

  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()

  allCountriesDF.show()

  val readMoviesDF: DataFrame =
    spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/movies.json")

  val select2MovieColumns: DataFrame =
    readMoviesDF.select(
      col("Title"),
      col("US_Gross")
    )

  select2MovieColumns.show()

  val select2MovieColumns2: DataFrame =
    readMoviesDF.select(
      col("Title"),
      col("Release_Date")
    )

  select2MovieColumns2.show()

  val moviesProfit =
    readMoviesDF.select(
      col("Title"),
      col("US_Gross"),
      col("Worldwide_Gross"),
      (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
    )

  val moviesProfitDF2 =
    readMoviesDF.selectExpr(
      "Title",
      "US_Gross",
      "Worldwide_Gross",
      "US_Gross +  Worldwide_Gross as Total_Gross"
    )

  val moviesProfitDF3 =
    readMoviesDF.select(
      "Title",
      "US_Gross",
      "Worldwide_Gross"
    ).withColumn("Total Gross", col("US_Gross") + col("Worldwide_Gross"))

  moviesProfit.show()

  println("**" * 10 + "Movies Profit v2")

  moviesProfitDF2.show()

  println("**" * 10 + "Movies Profit v3")

  moviesProfitDF3.show()

  println("**" * 10 + "atLeastMediocreComediesDF")

  val atLeastMediocreComediesDF =
    readMoviesDF.select("Title", "IMDB_Rating")
      .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6) // kinda like sql

  val atLeastMediocreComediesDF2 =
    readMoviesDF.select("Title", "IMDB_Rating")
      .where(col("Major_Genre") === "Comedy")
      .where(col("IMDB_Rating") > 6) // kinda like sql

  val atLeastMediocreComediesDF3 =
    readMoviesDF.select("Title", "IMDB_Rating")
      .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")

  atLeastMediocreComediesDF.show()
}
