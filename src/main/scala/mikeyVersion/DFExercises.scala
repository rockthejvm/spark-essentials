package mikeyVersion

import mikeyVersion.DataFramesBasics.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

object DFExercises {

  def main(args: Array[String]): Unit = {

    val sparkApp =
      SparkSession.builder()
        .appName("My phone spark app")
        .config("spark.master", "local")
        .getOrCreate()

    import spark.implicits._

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

    val createDF = sparkApp.createDataFrame(phones)

    val phonesToDF: DataFrame = phones.toDF("make", "model", "dimensions", "megapixels") // not sure why null pointer

    createDF.printSchema()
    phonesToDF.printSchema()
  }
}
