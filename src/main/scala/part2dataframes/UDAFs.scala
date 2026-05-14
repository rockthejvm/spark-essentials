package part2dataframes

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._

object UDAFs {

  val spark = SparkSession.builder()
    .master("local")
    .getOrCreate()

  // concatenate all Car names with a comma
  val carsDF = spark.read.json("src/main/resources/data/cars.json")

  // 1 - define a Scala function
  // col type, buffer, final type
  object Concatenator extends Aggregator[String, String, String] {

    override def zero: String = ""

    override def reduce(buffer: String, newValue: String): String = {
      if (buffer.isEmpty) newValue
      else if (newValue.isEmpty) buffer
      else s"$buffer, $newValue"
    }

    override def merge(b1: String, b2: String): String = {
      if (b1.isEmpty) b2
      else if (b2.isEmpty) b1
      else s"$b1, $b2"
    }

    override def finish(finalBuffer: String): String =
      finalBuffer

    override def bufferEncoder: Encoder[String] = Encoders.STRING
    override def outputEncoder: Encoder[String] = Encoders.STRING
  }

  // 2 - register as a UDAF
  val concatenatorUDAF = udaf(Concatenator)

  // 3 - apply it in a dataframe
  val allCarNamesDF = carsDF.select(concatenatorUDAF(col("Name")).as("All_Cars"))

  // CGR for values
  // [100, 110, 132, 198] =
  // [1.1, 1.2, 1.5]
  // (1.1 * 1.2 * 1.5) ^ (1/3) = ...

  // 1
  case class CGRBuffer(product: Double, nIntervals: Long, lastValue: Double)
  object CGR extends Aggregator[Double, CGRBuffer, Double] {
    override def zero: CGRBuffer =
      CGRBuffer(1.0, 0, -1.0)

    override def reduce(buffer: CGRBuffer, value: Double): CGRBuffer =
      if (buffer.lastValue < 0) buffer.copy(lastValue = value)
      else {
        val gr = value / buffer.lastValue
        CGRBuffer(buffer.product * gr, buffer.nIntervals + 1, value)
      }

    override def merge(b1: CGRBuffer, b2: CGRBuffer): CGRBuffer =
      CGRBuffer(b1.product * b2.product, b1.nIntervals + b2.nIntervals, b2.lastValue)

    override def finish(buffer: CGRBuffer): Double =
      math.pow(buffer.product, 1.0 / buffer.nIntervals)

    override def bufferEncoder: Encoder[CGRBuffer] = Encoders.product
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  // 2
  val cgrUDAF = udaf(CGR)

  // 3
  val msftCmgrDF = spark.read
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")
    .filter(col("symbol") === "GOOG")
    .agg(cgrUDAF(col("price")))

  def main(args: Array[String]): Unit = {
    msftCmgrDF.show()
  }
}
