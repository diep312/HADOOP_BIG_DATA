#!/usr/bin/env bash
# Khoi dong cum Hadoop pseudo-distributed + Spark History Server.
#
# Hai diem khac voi huong dan chuan:
#  1) Dung "hdfs/yarn --daemon start" thay cho start-dfs.sh/start-yarn.sh
#     => KHONG can cai va chay SSH server tren localhost.
#  2) Boc trong "setsid" de daemon nam o session moi. Neu khong, khi phien
#     WSL dong, toan bo tien trinh cua phien nhan SIGHUP va daemon Hadoop
#     se tu tat (log ghi: "RECEIVED SIGNAL 1: SIGHUP").
. "$HOME/opt/bigdata-env.sh"

start() { setsid "$@" < /dev/null > /dev/null 2>&1; }

echo ">> Khoi dong HDFS..."
start hdfs --daemon start namenode
start hdfs --daemon start datanode
start hdfs --daemon start secondarynamenode

echo ">> Khoi dong YARN..."
start yarn --daemon start resourcemanager
start yarn --daemon start nodemanager

echo ">> Khoi dong MapReduce JobHistory Server..."
start mapred --daemon start historyserver

echo ">> Cho HDFS thoat safemode..."
hdfs dfsadmin -safemode wait > /dev/null 2>&1

# Thu muc lam viec tren HDFS (chi co tac dung lan dau)
hdfs dfs -mkdir -p /user/"$(whoami)" /spark-logs /spark-jars /tmp/yarn-logs 2>/dev/null
hdfs dfs -chmod -R 1777 /tmp 2>/dev/null

echo ">> Khoi dong Spark History Server..."
start "$SPARK_HOME"/sbin/start-history-server.sh

sleep 3
echo ">> Tien trinh dang chay:"
jps 2>/dev/null | grep -v Jps | sort -k2
