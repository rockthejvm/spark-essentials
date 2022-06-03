# The official repository for the Rock the JVM Spark Essentials with Scala course

This repository contains the code we wrote during  [Rock the JVM's Spark Essentials with Scala](https://rockthejvm.com/course/spark-essentials) (Udemy version [here](https://udemy.com/spark-essentials)) Unless explicitly mentioned, the code in this repository is exactly what was caught on camera.

## How to install

- install [Docker](https://docker.com)
- either clone the repo or download as zip
- open with IntelliJ as an SBT project
- in a terminal window, navigate to the folder where you downloaded this repo and run `docker-compose up` to build and start the PostgreSQL container - we will interact with it from Spark
- in another terminal window, navigate to `spark-cluster/` 
- Linux/Mac users: build the Docker-based Spark cluster with
```
chmod +x build-images.sh
./build-images.sh
```
- Windows users: build the Docker-based Spark cluster with
```
build-images.bat
```
- when prompted to start the Spark cluster, go to the `spark-cluster` directory and run `docker-compose up --scale spark-worker=3` to spin up the Spark containers with 3 worker nodes

### A Note For Windows users: Adding Winutils

By default, Spark will be unable to write files using the local Spark executor. To write files, you will need to install the Windows Hadoop binaries, aka [winutils](https://github.com/cdarlint/winutils). You can take the latest binary (Hadoop 3.2 as of June 2022), or use Hadoop 2.7 as a fallback.

After you download winutils.exe, create a directory anywhere (e.g. `C:\\winutils`), then create a `bin` directory under that, then place the winutils executable there.

You will also need to set the `HADOOP_HOME` environment variable to your directory where you added `bin\winutils.exe`. In the example above, that would be `C:\\winutils`.

An alternative to setting the environment variable is to add this line at the beginning of every Spark application we write:

```scala
System.setProperty("hadoop.home.dir","C:\\hadoop") // replace C:\\hadoop with your actual directory
```

### How to start

Clone this repository and checkout the `start` tag by running the following in the repo folder:

```
git checkout start
```

### How to see the final code

Udemy students: checkout the `udemy` branch of the repo:
```
git checkout udemy
```

Premium students: checkout the master branch:
```
git checkout master
```

### How to run an intermediate state

The repository was built while recording the lectures. Prior to each lecture, I tagged each commit so you can easily go back to an earlier state of the repo!

The tags are as follows:

* `start`
* `1.1-scala-recap`
* `2.1-dataframes`
* `2.2-dataframes-basics-exercise`
* `2.4-datasources`
* `2.5-datasources-part-2`
* `2.6-columns-expressions`
* `2.7-columns-expressions-exercise`
* `2.8-aggregations`
* `2.9-joins`
* `2.10-joins-exercise`
* `3.1-common-types`
* `3.2-complex-types`
* `3.3-managing-nulls`
* `3.4-datasets`
* `3.5-datasets-part-2`
* `4.1-spark-sql-shell`
* `4.2-spark-sql`
* `4.3-spark-sql-exercises`
* `5.1-rdds`
* `5.2-rdds-part-2`

And for premium students, in addition:

* `6.1-spark-job-anatomy`
* `6.2-deploying-to-cluster`
* `7.1-taxi`
* `7.2-taxi-2`
* `7.3-taxi-3`
* `7.4-taxi-4`

When you watch a lecture, you can `git checkout` the appropriate tag and the repo will go back to the exact code I had when I started the lecture.

### For questions or suggestions

If you have changes to suggest to this repo, either
- submit a GitHub issue
- tell me in the course Q/A forum
- submit a pull request!
