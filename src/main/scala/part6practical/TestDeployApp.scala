package part6practical

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object TestDeployApp {

  def main(args: Array[String]): Unit = {
    /**
      * Movies.json as args(0)
      * GoodComedies.json as args(1)
      *
      * good comedy = genre == Comedy and IMDB > 6.5
      */

    if (args.length != 2) {
      println("Need input path and output path")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Test Deploy App")
      .getOrCreate()

    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json(args(0))

    val goodComediesDF = moviesDF.select(
      col("Title"),
      col("IMDB_Rating").as("Rating"),
      col("Release_Date").as("Release")
    )
      .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6.5)
      .orderBy(col("Rating").desc_nulls_last)

    goodComediesDF.show

    goodComediesDF.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(args(1))
  }

   /*
    * UPDATE: Rewritten for official Apache Spark Docker images and sbt packaging.
    *
    * Build a JAR to run a Spark application on the Docker cluster:
    *
    * Option A (sbt): Run `sbt package` from the project root.
    *   The JAR will be at target/scala-2.13/spark-essentials_2.13-0.3.jar
    *
    * Option B (IntelliJ):
    *   - Project Structure -> Artifacts, add artifact from "module with dependencies"
    *   - (important) check "copy to the output folder and link to manifest"
    *   - Build -> Build Artifacts... -> select the jar -> build
    *
    * Copy the JAR and movies.json to spark-apps/.
    * (the spark-apps folder is mapped to /opt/spark-apps in the containers)
    */

  /**
    * UPDATE: Updated Docker commands for official images (container name, Spark path).
    *
    * How to run the Spark application on the Docker cluster:
    *
    * 1. Start the cluster (from the repo root)
    *   docker compose up --scale spark-worker=3
    *
    * 2. Connect to the master node
    *   docker exec -it spark-master bash
    *
    * 3. Run the spark-submit command
    *   /opt/spark/bin/spark-submit \
    *     --class part6practical.TestDeployApp \
    *     --master spark://spark-master:7077 \
    *     --deploy-mode client \
    *     --verbose \
    *     /opt/spark-apps/spark-essentials_2.13-0.3.jar /opt/spark-apps/movies.json /opt/spark-apps/goodMovies
    */
}
