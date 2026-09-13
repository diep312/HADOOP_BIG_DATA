#!/usr/bin/env bash
# Dung toan bo cum theo thu tu nguoc lai
. "$HOME/opt/bigdata-env.sh"

"$SPARK_HOME"/sbin/stop-history-server.sh > /dev/null 2>&1
mapred --daemon stop historyserver
yarn  --daemon stop nodemanager
yarn  --daemon stop resourcemanager
hdfs  --daemon stop secondarynamenode
hdfs  --daemon stop datanode
hdfs  --daemon stop namenode
echo ">> Da dung cum. Tien trinh Java con lai:"
jps
