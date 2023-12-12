#!/bin/bash


base_dir=$(dirname $0)
set -e

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

if [[! -d ${LOG_DIR}]]; then
    mkdir -p "${LOG_DIR}"
fi

for file in "$base_dir"/../libs/meteor-cli-*.jar
do
  METEOR_JAR_FILE = ${file}
done

$JAVA -server ${JAVA_OPTS} -jar ${METEOR_JAR_FILE} "$@"