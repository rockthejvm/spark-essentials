package part4sql

object SparkShell extends App {

  /**
    * This file contains a dummy application where I added what we learned in the Spark Shell lecture.
    */

  /**
    *
    * To setup the Spark cluster in Docker:
    *
    *   1. Build the Docker containers
    *       ./build-images.sh
    *   2. Start a cluster with one master and 3 workers
    *       docker-compose up --scale spark-worker=3
    *
    * To open the Spark SQL shell in the master container:
    *   1. In another terminal window/tab, connect to the Docker container and open a regular shell
    *       docker exec -it docker-spark-cluster_spark-master_1 bash
    *   2. Execute the Spark SQL shell
    *       /spark/bin/spark-sql
    *
    *  If you want to inspect the files that Spark SQL writes, open another terminal window/tab
    *       docker exec -it docker-spark-cluster_spark-master_1 bash
    *
    * The Spark SQL shell allows you to write any SQL statement, from creating databases and tables, to selecting, to creating views etc.
    *
    * As discussed in the lecture, there is no difference between Spark SQL tables and DataFrames from Spark's point of view.
    * From our point of view as programmers:
    *   - DataFrames can only be accessed programmatically in Scala (or Java, Python or a language that Spark supports)
    *   - tables can be accessed in Spark SQL in the context of a database (which is just a form of organizing them)
    */



  /**
    * The commands we wrote in the lecture:
    *
    * // if you don't define a database, everything that you do will be related to the default database
    * show databases;
    *
    * // create a custom database
    * create database rtjvm;
    *
    * // switch to a database
    * use rtjvm;
    *
    * // show you the database you're using
    * select current_database();
    *
    * // create a table
    * create table persons(id integer, name string);
    *
    * // inserting data manually into a table
    * insert into persons values (1, "Martin Odersky"), (2, "Matei Zaharia")
    *
    * // selecting from a table
    * select * from persons;
    *
    * Once you have tables in your database(s), you can now write any kind of select statement, no matter how complex.
    *
    *
    */



  /**
    *
    * An interesting distinction is the difference between a MANAGED vs an EXTERNAL table.
    * For every table, Spark stores table metadata (column information, format, serialization, partitioning etc).
    *   - A MANAGED table means that Spark is responsible for storing both data and metadata.
    *     When you drop a table, you also drop its data.
    *   - An EXTERNAL table means that Spark is only responsible for metadata, whereas data is stored somewhere else (i.e. files, HDFS, other databases).
    *     When you drop an external table, Spark will only delete the metadata, meaning that you won't be able to reference or use it.
    *     Data is kept wherever it is, but it becomes your responsibility to store it/move it/migrate it etc.
    *
    * // show information about a table
    * describe extended persons; // you'll see MANAGED table
    *
    * Go to /spark/spark-warehouse/rtjvm.db and locate the persons folder - the table has a number of partitions.
    *
    * // you can change what format Spark can use and where to put it
    * create table flights(origin string, destination string) USING CSV OPTIONS(header true, path "/home/rtjvm/data/flights");
    *                                                          ^^ using CSV means store it in CSV form,
    *                                                          options is similar to writing a DF, e.g. "header true" == .option("header", "true") in DF-speak,
    *                                                          and path will store the table at that location and automatically make the table EXTERNAL
    * // you'll see EXTERNAL table
    * describe extended flights;
    *
    * // (not shown in the video)
    * // you can also explicitly create an external table
    * create external table persons_external(id integer, name string) row format delimited fields terminated by ',' location "/home/rtjvm/data/persons"
    * insert into persons_external (select * from persons);
   */

}
