#!/bin/bash

base_dir=$(dirname $0)
set -e

if [[ -z ${CONFIG_FILE}]]; then
  CONFIG_FILE = "${base_dir}/../config/broker.properties"
fi

if [[ -z ${JAVA_OPTS}]]; then
  JAVA_OPTS = "-Xms4g -Xmx4g -XX:+UseG1GC -Dfile.encoding=UTF-8 -Duser.timezone=Asia/Shanghai -XX:+MaxDirectMemorySize=16g -XX:+HeapDumpOnOutOfMemoryError -XX:+ExitOnOutOfMemoryError -XX:+HeapDumpPath=/tmp/heapdump.hprof =Dio.netty.maxDirectMemory=-1"
fi

if [[ -n ${JAVA_HOME}]]; then
    if [[-z ${JAVA_EXE}]]; then
        JAVA_EXE = ${JAVA_HOME}/bin/java
    fi
fi

if [[-z ${JAVA_EXE}]]; then
    JAVA_EXE = java
fi

if [[-z ${LOG_FILE}]]; then
    LOG_FILE = "${base_dir}/../config/logback.xml"
fi

if [[-z ${LOG_DIR}]]; then
    LOG_DIR = "${base_dir}/../logs"
fi

if [[! -d ${LOG_DIR}]]; then
    mkdir -p "${LOG_DIR}"
fi

for file in "$base_dir"/../libs/meteor-broker-*.jar
do
  METEOR_JAR_FILE = ${file}
done

if [[ -n $1 && $1 == 'foreground']]; then
    exec ${JAVA_EXE} -server -Dlogback.configurationFile=${LOG_FILE} -Dmeteor.log.dir=${LOG_DIR} ${JAVA_OPTS} -jar ${METEOR_JAR_FILE} -c ${CONFIG_FILE}
else
    nohup ${JAVA_EXE} -server -Dlogback.configurationFile=${LOG_FILE} -Dmeteor.log.dir=${LOG_DIR} ${JAVA_OPTS} -jar ${METEOR_JAR_FILE} -c ${CONFIG_FILE} > /dev/null 2>&1 &
fi