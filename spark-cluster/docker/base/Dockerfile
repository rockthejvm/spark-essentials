FROM eclipse-temurin:17-jdk
LABEL author="Daniel Ciocirlan" email="daniel@rockthejvm.com"
LABEL version="0.3"

ENV DAEMON_RUN=true
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SCALA_VERSION_BASE=2.13
ENV SCALA_VERSION=2.13.12
ENV SCALA_HOME=/usr/share/scala
ENV SPARK_HOME=/spark

RUN apt-get update && apt-get install -y curl vim wget software-properties-common ssh net-tools ca-certificates jq dbus-x11
RUN echo exit 0 > /usr/sbin/policy-rc.d

RUN cd "/tmp" && \
    wget --no-verbose "https://downloads.typesafe.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.tgz" && \
    tar xzf "scala-${SCALA_VERSION}.tgz" && \
    mkdir "${SCALA_HOME}" && \
    rm "/tmp/scala-${SCALA_VERSION}/bin/"*.bat && \
    mv "/tmp/scala-${SCALA_VERSION}/bin" "/tmp/scala-${SCALA_VERSION}/lib" "${SCALA_HOME}" && \
    ln -s "${SCALA_HOME}/bin/"* "/usr/bin/" && \
    rm -rf "/tmp/"*

# Add Dependencies for PySpark
RUN apt-get install -y python3 python3-pip python3-numpy python3-matplotlib python3-scipy python3-pandas python3-simpy
RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1

#Scala instalation
RUN export PATH="/usr/local/sbt/bin:$PATH" &&  apt update && apt install ca-certificates wget tar && mkdir -p "/usr/local/sbt" && wget -qO - --no-check-certificate "https://github.com/sbt/sbt/releases/download/v1.9.6/sbt-1.9.6.tgz" | tar xz -C /usr/local/sbt --strip-components=1 && sbt sbtVersion -Dsbt.rootdir=true

RUN wget --no-verbose https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION_BASE}.tgz && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION_BASE}.tgz \
      && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION_BASE} spark \
      && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION_BASE}.tgz



# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV PYTHONHASHSEED=1
