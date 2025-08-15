#!/bin/bash

base_dir=$(dirname $0)
set -e

if [[ -z ${CONFIG_FILE} ]]; then
  CONFIG_FILE="${base_dir}/../config/broker.properties"
fi

if [[ -z ${JAVA_OPTS} ]]; then
  JAVA_OPTS="-XX:MinHeapSize=8g -XX:InitialHeapSize=8g -XX:MaxHeapSize=8g -XX:-UseLargePages -XX:+UseZGC -XX:+ZGenerational -Dfile.encoding=UTF-8 -Duser.timezone=Asia/Shanghai -XX:MaxDirectMemorySize=32g -XX:+HeapDumpOnOutOfMemoryError -XX:+ExitOnOutOfMemoryError  -Dio.netty.tryReflectionSetAccessible=true -XX:HeapDumpPath=/tmp/heapdump.hprof -Dio.netty.maxDirectMemory=-1 --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --enable-preview"
fi

if [[ -n ${JAVA_HOME} ]]; then
    if [[ -z ${JAVA_EXE} ]]; then
        JAVA_EXE=${JAVA_HOME}/bin/java
    fi
fi

if [[ -z ${JAVA_EXE} ]]; then
    JAVA_EXE=java
fi

if [[ -z ${LOG_FILE} ]]; then
    LOG_FILE="${base_dir}/../config/logback-broker.xml"
fi

if [[ -z ${LOG_DIR} ]]; then
    LOG_DIR="${base_dir}/../logs"
fi

if [[ ! -d ${LOG_DIR} ]]; then
    mkdir -p "${LOG_DIR}"
fi

for file in "$base_dir"/../libs/meteor-broker-*.jar
do
  METEOR_JAR_FILE=${file}
done

echo "CONFIG_FILE=${CONFIG_FILE}"
echo "JAVA_OPTS=${JAVA_OPTS}"
echo "JAVA_HOME=${JAVA_HOME}"
echo "JAVA_EXE=${JAVA_EXE}"
echo "LOG_FILE=${LOG_FILE}"
echo "LOG_DIR=${LOG_DIR}"
echo "METEOR_JAR_FILE=${METEOR_JAR_FILE}"

if [[ -n $1 && $1 == 'foreground' ]]; then
    exec ${JAVA_EXE} -server -Dlogback.configurationFile=${LOG_FILE} -Dmeteor.log.dir=${LOG_DIR} ${JAVA_OPTS} -jar ${METEOR_JAR_FILE} -c ${CONFIG_FILE}
else
    nohup ${JAVA_EXE} -server -Dlogback.configurationFile=${LOG_FILE} -Dmeteor.log.dir=${LOG_DIR} ${JAVA_OPTS} -jar ${METEOR_JAR_FILE} -c ${CONFIG_FILE} > /dev/null 2>&1 &
fi