- Generate an executable JAR
- Supports both broker-server and command-line startup and execution
- It does not support client-server startup and execution because the client needs to dependency on the client JAR for
  launching
- local
  - ../mvnw clean install -DskipTests
  - ../mvnw assembly:single
  
- set JVM(ZGC) please see https://wiki.openjdk.org/display/zgc/Main