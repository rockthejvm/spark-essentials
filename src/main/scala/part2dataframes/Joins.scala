package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, max, col}

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // inner joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

  // outer joins
  // left outer = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  // right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  // outer join = everything in the inner join + all the rows in BOTH tables, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti-joins = everything in the left DF for which there is NO row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")


  // things to bear in mind
  // guitaristsBandsDF.select("id", "band").show // this crashes

  // option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  // using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

  /**
    * Exercises
    *
    * 1. show all employees and their max salary
    * 2. show all employees who were never managers
    * 3. find the job titles of the best paid 10 employees in the company
    */

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  // 1
  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")

  // 2
  val empNeverManagersDF = employeesDF.join(
    deptManagersDF,
    employeesDF.col("emp_no") === deptManagersDF.col("emp_no"),
    "left_anti"
  )

  // 3
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeesSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")

  bestPaidJobsDF.show()
}

