![Build project](https://github.com/shallowx/meteor/blob/master/docs/badge.svg)

## Meteor Project

Meteor is a distributed messaging and streaming platform developed based on direct memory, with low latency, high
performance, high reliability, trillion-level capacity, flexible expansion and other features

## Performance

- Better throughput, lower latency
- Less resource consumption
- Minimized unnecessary memory copy

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