[English](./README.md) | [中文](./README_ZH.md)

# CED-DB
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Version](https://maven-badges.herokuapp.com/maven-central/org.apache.iotdb/iotdb-parent/badge.svg)](http://search.maven.org/#search|gav|1|g:"org.apache.iotdb")
![](https://img.shields.io/badge/java--language-1.8%20%7C%2011%20%7C%2017-blue.svg)

# Overview
CED-DB (Cloud-Edge-Device DataBase) is a cloud-edge-device collaborative time-series database management system that provides users with data collection, storage, and query capabilities. By enabling seamless collaboration between cloud servers, edge devices, and sensors, CED-DB efficiently handles massive data processing, storage, and complex queries in the industrial IoT domain. Additionally, it can offload query tasks to the cloud, thereby reducing query pressure on edge devices.

# Main Features
The main features of CED-DB are as follows:

1. Hierarchical Architecture and Collaborative Computing.
* The cloud provides high-performance query capabilities, advanced analytics, and machine learning capabilities.
* The edge is responsible for local data preprocessing, preliminary analysis, data filtering, and compression, reducing computational and storage pressure on the cloud.
* The device directly collects time-series data from sensors, offering low-latency processing and local storage capabilities.
2. Flexible Query and Analysis Capabilities
* When the query load at the edge becomes too high, CED-DB seamlessly offloads queries to the cloud for processing. Once the edge load returns to normal, the queries are switched back to the edge, enabling flexible query switching.
3. Seamless Integration with Advanced Open-Source Ecosystems
* CED-DB shares the same origin as IoTDB and fully integrates all of IoTDB's functionalities.
* Additionally, it supports LOADS database web-based demos for enhanced usability.
4. Low Learning Curve
* CED-DB uses IoTDB's native query language, supporting SQL-like syntax, JDBC standard API, and easy-to-use import/export tools, making it easy to learn and use.

<!-- TOC -->
## Outline

- [CED-DB](#ced-db)
- [Overview](#overview)
- [Main Features](#main-features)
  - [Outline](#outline)
- [Quick Start](#quick-start)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
    - [Building from Source](#building-from-source)
      - [Configurations](#configurations)
  - [Start](#start)
    - [Start CED-DB](#start-ced-db)
    - [Use CED-DB](#use-ced-db)
      - [Use Cli](#use-cli)
      - [Basic Commands](#basic-commands)
    - [Stop CED-DB](#stop-ced-db)
- [Contact Us](#contact-us)
- [Statement](#statement)


# Quick Start

This short guide will walk you through the basic process of using CED-DB. For more detailed information, please feel free to contact us.

## Prerequisites
To use CED-DB, you need:
1. Java >= 1.8 (Versions 11 to 17 have been verified; Java 15 is recommended. Ensure the environment variables are set correctly).
2. Maven >= 3.6.
3. Set max open files to 65,535 to avoid the "too many open files" error.
4. (Optional) Set `somaxconn` to 65,535 to prevent "connection reset" errors under high loads.
    ```
    # Linux
    > sudo sysctl -w net.core.somaxconn=65535
   
    # FreeBSD or Darwin
    > sudo sysctl -w kern.ipc.somaxconn=65535
    ```
## Installation

Here in the Quick Start, we give a brief introduction of using source code to install CED-DB.

## Build from source

### Prepare Thrift compiler

Skip this chapter if you are using Windows.

As we use Thrift for our RPC module (communication and
protocol definition), we involve Thrift during the compilation, so Thrift compiler 0.13.0 (or
higher) is required to generate Thrift Java code. Thrift officially provides binary compiler for
Windows, but unfortunately, they do not provide that for Unix OSs.

If you have permission to install new softwares, use `apt install` or `yum install` or `brew install`
to install the Thrift compiler (If you already have installed the thrift compiler, skip this step).
Then, you may add the following parameter
when running Maven: `-Dthrift.download-url=http://apache.org/licenses/LICENSE-2.0.txt -Dthrift.exec.absolute.path=<YOUR LOCAL THRIFT BINARY FILE>`.

If not, then you have to compile the thrift compiler, and it requires you install a boost library first.
Therefore, we compiled a Unix  compiler ourselves and put it onto GitHub, and with the help of a
maven plugin, it will be  downloaded automatically during compilation.
This compiler works fine with gcc8 or later, Ubuntu  MacOS, and CentOS, but previous versions
and other OSs are not guaranteed.

If you can not download the thrift compiler automatically because of network problem, you can download
it yourself, and then either:
rename your thrift file to `{project_root}\thrift\target\tools\thrift_0.12.0_0.13.0_linux.exe`;
or, add Maven commands:
`-Dthrift.download-url=http://apache.org/licenses/LICENSE-2.0.txt -Dthrift.exec.absolute.path=<YOUR LOCAL THRIFT BINARY FILE>`.

### Compile CED-DB

You can download the source code from:
```
https://github.com/MDC-LOADS/Cloud-Edge-Device-DataBase.git
```
The default dev branch is the Edge branch, If you want to use the Cloud branch:
```
git checkout CED-DB-Cloud
```
If you want to use the Edge branch：
```
git checkout CED-DB-Edge
```

### Build CED-DB from source

Under the root path of Cloud-Edge-Device-DateBase:
```
> mvn clean package -pl distribution -am -DskipTests -Dcheckstyle.skip=true
```
After being built, the IoTDB distribution is located at the folder: "distribution/target".

### Only build cli

Under the iotdb/iotdb-client path:

```
> mvn clean package -pl cli -am -DskipTests
```

After being built, the IoTDB cli is located at the folder "cli/target".

### Build Others

Using `-P compile-cpp` for compiling cpp client (For more details, read client-cpp's Readme file.)

**NOTE: Directories "`thrift/target/generated-sources/thrift`", "`thrift-sync/target/generated-sources/thrift`",
"`thrift-cluster/target/generated-sources/thrift`", "`thrift-influxdb/target/generated-sources/thrift`"
and "`antlr/target/generated-sources/antlr4`" need to be added to sources roots to avoid compilation errors in the IDE.**

**In IDEA, you just need to right click on the root project name and choose "`Maven->Reload Project`" after
you run `mvn package` successfully.**

### Configurations

configuration files are under "conf" folder

* environment config module (`datanode-env.bat`, `datanode-env.sh`),
* system config module (`iotdb-datanode.properties`)
* log config module (`logback.xml`).

## Start

You can go through the following steps to test the installation. If there is no error returned after execution, the installation is completed.

### Start CED-DB

You can start CED-DB by running the script in the sbin folder. The specific steps are as follows (Linux):

1. Launch the Edge version:

You need to add the following command below `source "$(dirname "$0")/iotdb-common.sh"` in the `distribution/target/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/sbin/start-confignode.sh` file:

```
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5201"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DCONFIGNODE_CONF=./conf_edge"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -Dlogback.configurationFile=./conf_edge/logback-confignode.xml"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DTSFILE=./conf_edge"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -Dname=iotdb/.ConfigNodeEdge"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DIOTDB_CONF=./conf_edge"
```
Add the following command below `"$(dirname "$0")/iotdb-common.sh" `in the `distribution/target/apache-iotdb-1.3.0-SNAPSHOT-datanode-bin/apache-iotdb-1.3.0-SNAPSHOT-datanode-bin/sbin/start-datanode.sh` file:

```
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5211"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DCONFIGNODE_CONF=./conf_edge"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -Dlogback.configurationFile=./conf_edge/logback-datanode.xml"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DTSFILE=./conf_edge"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -Dname=iotdb/.DataNodeEdge"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DIOTDB_CONF=./conf_edge"
```

Run ConfigNode-Edge:
```
sudo distribution/target/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/sbin/start-confignode.sh
```
Run DataNode-Edge:

```
sudo distribution/target/apache-iotdb-1.3.0-SNAPSHOT-datanode-bin/apache-iotdb-1.3.0-SNAPSHOT-datanode-bin/sbin/start-datanode.sh
```

2. Launch the Cloud version:

You need to add the following command below `source "$(dirname "$0")/iotdb-common.sh"` in the `distribution/target/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/sbin/start-confignode.sh` file:

```
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5200"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DCONFIGNODE_CONF=./conf_cloud"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -Dlogback.configurationFile=./conf_cloud/logback-confignode.xml"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DTSFILE=./conf_cloud"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -Dname=iotdb/.ConfigNodeEdge"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DIOTDB_CONF=./conf_cloud"
```

Add the following command below `"$(dirname "$0")/iotdb-common.sh" `in the `distribution/target/apache-iotdb-1.3.0-SNAPSHOT-datanode-bin/apache-iotdb-1.3.0-SNAPSHOT-datanode-bin/sbin/start-datanode.sh` file:
```
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5210"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DCONFIGNODE_CONF=./conf_cloud"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -Dlogback.configurationFile=./conf_cloud/logback-datanode.xml"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DTSFILE=./conf_cloud"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -Dname=iotdb/.DataNodeEdge"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DIOTDB_CONF=./conf_cloud"
```
Run ConfigNode-Cloud:
```
sudo distribution/target/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/sbin/start-confignode.sh
```
Run DataNode-Cloud:
```
sudo distribution/target/apache-iotdb-1.3.0-SNAPSHOT-datanode-bin/apache-iotdb-1.3.0-SNAPSHOT-datanode-bin/sbin/start-datanode.sh
```
When you need to use it on two devices, you need to modify `localhost` in `iotdb-core/datanode/src/main/java/zyh/service/LoadDetection.java`, `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/execution/operator/source/AbstractSeriesAggregationScanOperator.java`, and `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/execution/operator/source/SeriesScanOperator.java` to the corresponding IP address.

### Use CED-DB

#### Use Cli

CED-DB provides different ways to interact with the server, here we will introduce the basic steps to insert and query data using the Cli tool.

After installing CED-DB, there is a default user `root`, and its default password is also `root`. Users can use this
default user to log in to Cli and use CED-DB. The startup script of Cli is the start-cli script in the sbin folder.
When executing the script, users should specify the IP, port, USER_NAME and password. The default parameters are `-h 127.0.0.1 -p 6667 -u root -pw root`.

Here is the command for starting the Cli:

```
> distribution/target/apache-iotdb-1.3.0-SNAPSHOT-cli-bin/apache-iotdb-1.3.0-SNAPSHOT-cli-bin/sbin/start-cli.sh -p 6667
```

The command line cli is interactive, so you should see the welcome logo and statements if everything is ready:

```
   _____   ______  _____   ______     ______
  /  ___| |  ____||  __ \  |_   _ `.  |_   _ \
 |  |     | |__   | |  | |   | | `. \  | |_) |
 |  |  _  |  __|  | |  | |   | |  | |  |  __'.
 \  \_| | | |____ | |__| |  _| |_.' / _| |__) |
  \____/  |______||_____/  |______.' |_______/ version x.x.x

CED-DB> login successfully
CED-DB>
```
#### Basic commands

Now, let us introduce the way of creating timeseries, inserting data and querying data.

The commands of CED-DB are the same as those of IoTDB. The data in CED-DB is organized as timeseries. Each timeseries includes multiple data-time pairs, and is owned by a database. Before defining a timeseries, we should define a database using CREATE DATABASE first, and here is an example:

```
CED-DB> CREATE DATABASE root.ln
```

We can also use SHOW DATABASES to check the database being created:

```
CED-DB> SHOW DATABASES
+-------------+
|     Database|
+-------------+
|      root.ln|
+-------------+
Total line number = 1
```

After the database is set, we can use CREATE TIMESERIES to create a new timeseries. When creating a timeseries, we should define its data type and the encoding scheme. Here we create two timeseries:

```
CED-DB> CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN
CED-DB> CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE
```

In order to query the specific timeseries, we can use SHOW TIMESERIES <Path>. <Path> represent the location of the timeseries. The default value is "null", which queries all the timeseries in the system(the same as using "SHOW TIMESERIES root"). Here are some examples:

1. Querying all timeseries in the system:

```
CED-DB> SHOW TIMESERIES
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                   Timeseries|Alias|Database|DataType|Encoding|Compression|Tags|Attributes|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln.wf01.wt01.temperature| null|      root.ln|   FLOAT|     RLE|     SNAPPY|null|      null|
|     root.ln.wf01.wt01.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
Total line number = 2
```

2. Querying a specific timeseries(root.ln.wf01.wt01.status):

```
CED-DB> SHOW TIMESERIES root.ln.wf01.wt01.status
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
|              timeseries|alias|database|dataType|encoding|compression|tags|attributes|
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln.wf01.wt01.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
Total line number = 1
```

Insert timeseries data is a basic operation of CED-DB, you can use ‘INSERT’ command to finish this. Before insertion, you should assign the timestamp and the suffix path name:

```
CED-DB> INSERT INTO root.ln.wf01.wt01(timestamp,status) values(100,true);
CED-DB> INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values(200,false,20.71)
```

The data that you have just inserted will display as follows:

```
CED-DB> SELECT status FROM root.ln.wf01.wt01
+------------------------+------------------------+
|                    Time|root.ln.wf01.wt01.status|
+------------------------+------------------------+
|1970-01-01T00:00:00.100Z|                    true|
|1970-01-01T00:00:00.200Z|                   false|
+------------------------+------------------------+
Total line number = 2
```

You can also query several timeseries data using one SQL statement:

```
CED-DB> SELECT * FROM root.ln.wf01.wt01
+------------------------+-----------------------------+------------------------+
|                    Time|root.ln.wf01.wt01.temperature|root.ln.wf01.wt01.status|
+------------------------+-----------------------------+------------------------+
|1970-01-01T00:00:00.100Z|                         null|                    true|
|1970-01-01T00:00:00.200Z|                        20.71|                   false|
+------------------------+-----------------------------+------------------------+
Total line number = 2
```

To change the time zone in Cli, you can use the following SQL:

```
CED-DB> SET time_zone=+08:00
Time zone has set to +08:00
CED-DB> SHOW time_zone
Current time zone: Asia/Shanghai
```

Add then the query result will show using the new time zone.

```
CED-DB> SELECT * FROM root.ln.wf01.wt01
+-----------------------------+-----------------------------+------------------------+
|                         Time|root.ln.wf01.wt01.temperature|root.ln.wf01.wt01.status|
+-----------------------------+-----------------------------+------------------------+
|1970-01-01T08:00:00.100+08:00|                         null|                    true|
|1970-01-01T08:00:00.200+08:00|                        20.71|                   false|
+-----------------------------+-----------------------------+------------------------+
Total line number = 2
```

The commands to exit the Cli are:

```
CED-DB> quit
or
CED-DB> exit
```

Since the commands of CED-DB and IoTDB are the same, for more information about the commands supported by IoTDB SQL, see the [IoTDB User Guide](https://iotdb.apache.org/zh/UserGuide/Master/QuickStart/QuickStart.html).

### Stop CED-DB

The server can be stopped with "ctrl-C" or the following script:
```
> distribution/target/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/sbin/stop-standalone.sh
```

