# Spark Cluster (Docker)

<!-- UPDATE: Completely rewritten — replaced custom Docker image build system with official Apache Spark images.
     No more build-images.sh, custom Dockerfiles, or dos2unix workarounds. -->

## Quick Start

From the **root** of this repository:

```bash
docker compose up --scale spark-worker=3
```

This starts:
- A **PostgreSQL** database (port 5432) pre-loaded with the course data
- A **Spark Master** (Web UI at http://localhost:9090, Spark protocol on port 7077)
- **3 Spark Workers** connected to the master

All services use official Docker images — no build step required.

## Accessing the Cluster

```bash
# Open a shell on the master node
docker exec -it spark-master bash

# Launch the Spark shell (Scala)
/opt/spark/bin/spark-shell --master spark://spark-master:7077

# Launch the Spark SQL shell
/opt/spark/bin/spark-sql --master spark://spark-master:7077
```

## Submitting a JAR

1. Build your JAR (e.g. `sbt package`) and copy it to `spark-cluster/apps/`
2. Copy any data files to `spark-cluster/data/`
3. Submit:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --class your.main.Class \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark-apps/your-jar.jar [args...]
```

The `apps/` and `data/` directories are mounted into the containers at `/opt/spark-apps` and `/opt/spark-data`.

## Resource Allocation

- Default CPU cores per worker: 1
- Default RAM per worker: 1G
- Modify these in the root `docker-compose.yml` under the `spark-worker` service environment variables.

## Stopping

```bash
docker compose down      # stop containers
docker compose down -v   # stop and remove volumes
```
