#!/usr/bin/env bash
# ===== SPARK_HOME/conf/spark-env.sh =====
export JAVA_HOME=/home/thomm/opt/jdk11
export HADOOP_HOME=/home/thomm/opt/hadoop

# Spark doc cau hinh cum HDFS/YARN tu hai bien nay
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export YARN_CONF_DIR=${HADOOP_HOME}/etc/hadoop

# Ban Spark "bin-hadoop3" khong kem day du jar Hadoop server-side,
# nap classpath Hadoop cua he thong de dung dung phien ban 3.3.6
export SPARK_DIST_CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath)

# WSL2 co the resolve hostname ra IP la => ep ve loopback
export SPARK_LOCAL_IP=127.0.0.1

export SPARK_LOG_DIR=/home/thomm/opt/logs/spark
export SPARK_PID_DIR=/home/thomm/hadoopdata/pids
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
