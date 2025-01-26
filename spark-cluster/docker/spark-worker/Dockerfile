FROM spark-base:latest

RUN apt-get update && apt-get install -y dos2unix
COPY start-worker.sh /
RUN dos2unix /start-worker.sh && apt-get --purge remove -y dos2unix && rm -rf /var/lib/apt/lists/*

ENV SPARK_WORKER_WEBUI_PORT=8081
ENV SPARK_WORKER_LOG=/spark/logs
ENV SPARK_MASTER="spark://spark-master:7077"

EXPOSE 8081

CMD ["/bin/bash", "/start-worker.sh"]
