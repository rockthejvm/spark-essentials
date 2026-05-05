# The official repository for the Rock the JVM Spark Essentials with Scala course

<!-- UPDATE: Rewritten for Spark 4.1.1 — simplified Docker setup (official images), updated prerequisites, removed build-images step -->

This repository contains the code we wrote during [Rock the JVM's Spark Essentials with Scala](https://rockthejvm.com/course/spark-essentials) (Udemy version [here](https://udemy.com/spark-essentials)). Unless explicitly mentioned, the code in this repository is exactly what was caught on camera.

## Prerequisites

- **Java**: JDK 17 or 21 (recommend [Eclipse Temurin 21](https://adoptium.net/))
- **Docker**: [Docker Desktop](https://docker.com) (Mac/Windows) or Docker Engine (Linux)
- **IDE**: [IntelliJ IDEA](https://www.jetbrains.com/idea/) with the Scala plugin
- **Windows-specific**: See [HadoopWindowsUserSetup.md](/HadoopWindowsUserSetup.md) or use WSL 2 (recommended)

> **Note**: Spark 4.x requires Java 17 or 21. Java 8 and 11 are **no longer supported**.

## How to install

- Install [Docker](https://docker.com)
- Install JDK 17 or 21 ([Eclipse Temurin](https://adoptium.net/) recommended)
- Either clone the repo or download as zip
- Open with IntelliJ as an SBT project
- Windows users: you need to set up some Hadoop-related configs — use [this guide](/HadoopWindowsUserSetup.md)
- In a terminal window, navigate to the folder where you downloaded this repo and run `docker compose up` to start PostgreSQL and the Spark cluster
- To spin up multiple Spark workers: `docker compose up --scale spark-worker=3`
- Access the Spark Master UI at http://localhost:9090

### Connecting to PostgreSQL

```bash
docker exec -it postgres psql -U docker -d rtjvm
```

### Connecting to the Spark Cluster

```bash
# Open a shell on the Spark master
docker exec -it spark-master bash

# Launch the Spark shell
/opt/spark/bin/spark-shell --master spark://spark-master:7077

# Launch the Spark SQL shell
/opt/spark/bin/spark-sql --master spark://spark-master:7077
```

### Submitting a JAR to the Cluster

1. Build the JAR: `sbt package` (or use IntelliJ Build Artifacts)
2. Copy the JAR and any data files to `spark-apps/`
3. Submit from inside the master container:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --class part6practical.TestDeployApp \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark-apps/spark-essentials_2.13-0.3.jar /opt/spark-apps/movies.json /opt/spark-apps/goodMovies
```

## How to use the code

Clone this repository and checkout the `start` tag by running the following in the repo folder:

```
git checkout start
```

### How to see the final code

Udemy students: checkout the `udemy` branch of the repo:
```
git checkout udemy
```

Rock the JVM students: checkout the master branch:
```
git checkout master
```

### For questions or suggestions

If you have changes to suggest to this repo, either
- submit a GitHub issue
- tell me in the course Q/A forum
- submit a pull request!
