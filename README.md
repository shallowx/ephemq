![Build project](https://github.com/shallowx/meteor/blob/master/docs/badge.svg)

## Meteor Project

Meteor is a distributed messaging and streaming platform built on direct memory access. 
It offers low latency, high performance, high reliability, trillion-level throughput, and flexible scalability, making it ideal for demanding, large-scale systems.

## Performance

- Higher throughput and lower latency
- Reduced resource consumption
- Minimized unnecessary memory copying

## Links

* [Web Site]()
* [Downloads]()
* [Documentation]()
* [Official Discord server]()

## Security

- Complete SSL/TLS and StartTLS support

## How to build

For the detailed information about building and developing Meteor, please visit the [developer guide](). This page only
gives very basic information.

You require the following to build Meteor:

* Latest stable [OpenJDK 21](https://adoptium.net/)
* Latest stable [Apache Maven](https://maven.apache.org/)
* Latest stable [Apache Zookeeper](https://zookeeper.apache.org/)
* run maven command line `./mvnw clean install -DskipTests -Dcheckstyle.skip=true -U`

Note that this is build-time requirement. JDK-21 is enough to run your Meteor-based application.

## Branches to look

Development of all versions takes place in each branch whose name is identical to <majorVersion>.<minorVersion>. For
example, the development of master resides in the branch 'master' respectively.