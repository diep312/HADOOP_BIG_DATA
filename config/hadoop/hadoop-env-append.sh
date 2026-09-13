
# ===== BIGDATA-LAB-CONFIG-BEGIN =====
export JAVA_HOME=/home/thomm/opt/jdk11
export HADOOP_HOME=/home/thomm/opt/hadoop
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export HADOOP_LOG_DIR=/home/thomm/opt/logs/hadoop
export HADOOP_PID_DIR=/home/thomm/hadoopdata/pids

# Heap cho tung daemon (may thu nghiem RAM han che nen dat thu cong)
export HDFS_NAMENODE_OPTS="-Xmx1024m"
export HDFS_DATANODE_OPTS="-Xmx1024m"
export HDFS_SECONDARYNAMENODE_OPTS="-Xmx512m"
export YARN_RESOURCEMANAGER_OPTS="-Xmx1024m"
export YARN_NODEMANAGER_OPTS="-Xmx1024m"

export HADOOP_OPTS="${HADOOP_OPTS} -Djava.library.path=${HADOOP_HOME}/lib/native"
# ===== BIGDATA-LAB-CONFIG-END =====
