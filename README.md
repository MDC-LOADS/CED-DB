[English](./README.md) | [中文](./README_ZH.md)

# CED-DB
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Version](https://maven-badges.herokuapp.com/maven-central/org.apache.iotdb/iotdb-parent/badge.svg)](http://search.maven.org/#search|gav|1|g:"org.apache.iotdb")
![](https://img.shields.io/badge/java--language-1.8%20%7C%2011%20%7C%2017-blue.svg)

# Introduction
CED-DB (Cloud-Edge-Device DataBase) is a cloud-edge-device collaborative time-series database management system that provides users with data collection, storage, and query capabilities. By enabling collaboration among cloud servers, edge devices, and sensors, CED-DB can satisfy the demands of massive data processing, storage, and complex query analysis in industrial IoT scenarios, while also supporting query task migration to relieve query pressure on edge devices.

# Reproducing the Experiments

This document provides a complete step-by-step guide for reproducing the experiments in this project. You can also access our data and configuration files via [OneDrive](https://1drv.ms/f/c/3ba3d79950bd0ad5/IgAjBcLnAGmeSaRlG3nuCm_qAeZbgMB621oM2ZefQOuxqYs?e=9hjxsB).

---
## 1. Generate Data Using IoT-Benchmark

Use the following configuration to generate the experimental dataset with [IoT-Benchmark](https://github.com/thulab/iot-benchmark). We include the necessary configuration parameters below. For detailed usage instructions, please refer to [IoT-Benchmark](https://github.com/thulab/iot-benchmark):

```properties
DB_SWITCH=IoTDB-130-SESSION_BY_TABLET
IoTDB_DIALECT_MODE=tree
LOOP=2000000
BATCH_SIZE_PER_WRITE=400
DEVICE_NUMBER=50
SENSOR_NUMBER=100
GROUP_NUMBER=5
POINT_STEP=1
START_TIME=2024-01-01T00:00:00+08:00
DOUBLE_LENGTH=15
```

---

## 2. Data Synchronization or Import

After the data is generated, it needs to be imported into the experimental environment. Available options include:

- Use **LOADS** for hot synchronization  
- Use the TsFile [Import/Export Tool](https://iotdb.apache.org/UserGuide/V1.3.x/Tools-System/Data-Import-Tool-1-3-4.html)  
- Use the IoTDB [Data Synchronization Tool](https://iotdb.apache.org/UserGuide/V1.3.x/User-Manual/Data-Sync_apache.html)

---

## 3. Configure CEDCQ

Modify the configuration according to the actual deployment environment. See the CEDCQ section below for details. This includes:

- Node IP addresses and ports  
- Data paths  
- Execution parameters  

---

## 4. Run the Experiments

After completing the steps above, you can run query experiments using CEDCQ.

---

## Notes

- Results may vary slightly under different hardware and network environments  
- Please ensure that the data and configuration are correct before running the experiments  

---

## Workflow Summary

1. Generate data  
2. Import or synchronize data  
3. Configure CEDCQ  
4. Run queries  

# Key Features

The main features of CED-DB are as follows:

1. **Hierarchical architecture and collaborative computing.** The cloud side of CED-DB provides high-performance query capabilities, advanced analytics, and machine learning capabilities. The edge side is responsible for local data preprocessing, preliminary analysis, data filtering, and compression, thereby reducing cloud-side computation and storage pressure. The device side directly collects time-series data from devices, supports low-latency processing, and can provide local storage.
2. **Flexible query and analytics capabilities.** When the query load on the edge side becomes too high, CED-DB can seamlessly send query tasks to the cloud for execution, and switch the queries back to the edge side once the edge load recovers, enabling flexible query migration.
3. **Seamless integration with an advanced open-source ecosystem.** CED-DB shares the same origin as IoTDB. While integrating all IoTDB features, it also supports the [LOADS](https://github.com/MDC-LOADS/LOADS) database web demo.
4. **Very low learning cost.** It uses the native IoTDB language and supports SQL-like syntax, the standard JDBC API, and easy-to-use import/export tools.

<!-- TOC -->

## Table of Contents
- [CED-DB](#ced-db)
- [Introduction](#introduction)
- [Key Features](#key-features)
  - [Table of Contents](#table-of-contents)
- [Quick Start](#quick-start)
  - [Environment Preparation](#environment-preparation)
  - [Installation](#installation)
    - [Build from Source](#build-from-source)
      - [Configuration](#configuration)
  - [Getting Started](#getting-started)
    - [Start CED-DB](#start-ced-db)
    - [Use CED-DB](#use-ced-db)
      - [Use the CLI](#use-the-cli)
      - [Basic Commands](#basic-commands)
    - [Stop CED-DB](#stop-ced-db)
- [Contact Us](#contact-us)
- [Disclaimer](#disclaimer)

<!-- /TOC -->

# Quick Start

This short guide walks you through the basic process of using CED-DB. For a more detailed introduction, please contact us.

## Environment Preparation
To use CED-DB, you need:

1. Java >= 1.8 (versions 11 to 17 have been verified to work, and version 15 is recommended. Please make sure your environment variables are configured correctly).
2. Maven >= 3.6.
3. Set `max open files` to 65535 to avoid the `"too many open files"` error.
4. (Optional) Set `somaxconn` to 65535 to avoid `"connection reset"` errors when the system is under high load.
    ```
    # Linux
    > sudo sysctl -w net.core.somaxconn=65535

    # FreeBSD or Darwin
    > sudo sysctl -w kern.ipc.somaxconn=65535
    ```

## Installation

In this quick start, we briefly introduce how to install CED-DB from source.

## Build from Source

### Preparing the Thrift Compiler

If you are using Windows, please skip this subsection.

We use Thrift as the RPC module to provide client-server communication and protocol support. Therefore, during compilation, we need Thrift 0.13.0 (or higher) to generate the corresponding Java code. Thrift only provides a binary compiler for Windows; on Unix systems, it needs to be compiled from source.

If you have installation privileges, you can install the thrift compiler using `apt install`, `yum install`, or `brew install`, and then add the following parameters to the build command below:
`-Dthrift.download-url=http://apache.org/licenses/LICENSE-2.0.txt -Dthrift.exec.absolute.path=<path to your thrift executable>`.

We have also precompiled a Thrift compiler and uploaded it to GitHub. With the help of a Maven plugin, it can be downloaded automatically during compilation. (For example, if you are compiling on Linux, you may ignore this paragraph.)
This precompiled Thrift compiler works on gcc8, Ubuntu, CentOS, and macOS, but has not yet been verified on lower gcc versions or other operating systems.
If you repeatedly get errors indicating that the thrift file cannot be downloaded due to network issues, you need to download it manually and place the compiler in the directory `{project_root}\thrift\target\tools\thrift_0.12.0_0.13.0_linux.exe`.
If you place it elsewhere, you need to add the following parameter when running the Maven command:
`-Dthrift.download-url=http://apache.org/licenses/LICENSE-2.0.txt -Dthrift.exec.absolute.path=<path to your thrift executable>`.

If you are familiar enough with Maven, you may also modify our root `pom.xml` directly to avoid passing the above parameters every time you compile.
The official Thrift website is: https://thrift.apache.org/

### Prepare the Source Code

Clone the source code from Git:
```
https://github.com/MDC-LOADS/CED-DB.git
```

The default main branch is the Edge branch. If you want to use the Cloud version, switch to the following tag:
```
git checkout CED-DB-Cloud-2.0
```

If you want to use the Edge version, switch to the following tag:
```
git checkout CED-DB-Edge-2.0
```

If you want to use a distributed environment, you need to modify the following configuration files:

# Edge Version
Configure the confignode in `/dev-conf/iotdb-system.properties`:
```
cn_internal_address=edge_ip
cn_seed_config_node=edge_ip:10710
```

Configure the datanode in `/dev-conf/iotdb-system.properties`:
```
dn_rpc_address=0.0.0.0
dn_internal_address=0.0.0.0
dn_seed_config_node=edge_ip:10710
```

# Cloud Version
Configure the confignode in `/dev-conf/iotdb-system.properties`:
```
cn_internal_address=cloud_ip
cn_seed_config_node=cloud_ip:10710
```

Configure the datanode in `/dev-conf/iotdb-system.properties`:
```
dn_rpc_address=0.0.0.0
dn_internal_address=0.0.0.0
dn_seed_config_node=cloud_ip:10710
```

### CEDCQ

If you want to enable the CEDCQ function, you need to modify the following configuration files:

# Edge Version
Configure `/dev-conf/iotdb-colquery.properties`:
```
colquery.bind.ip=0.0.0.0
colquery.local.ip=edge_ip
colquery.remote.ip=cloud_ip
colquery.local.rpc.port=9090
colquery.remote.rpc.port=9090
colquery.local.mpp.port=10740
colquery.remote.mpp.port=10740
colquery.col.query.wait=3. # controls the collaboration timing
colquery.iscol.query=true
```

# Cloud Version
Configure `/dev-conf/iotdb-colquery.propertie`:
```
colquery.bind.ip=0.0.0.0
colquery.local.ip=cloud_ip
colquery.remote.ip=edge_ip
colquery.local.rpc.port=9090
colquery.remote.rpc.port=9090
colquery.local.mpp.port=10740
colquery.remote.mpp.port=10740
```

### Compile CED-DB from Source

Run the following command in the root directory of Cloud-Edge-Device-DateBase:

```
> sudo clean package -pl distribution -am -DskipTests -Dcheckstyle.skip=true -Dspotless.skip=true
```

If you need to use a proxy, you can run the following command:

```
> mvn clean package -pl distribution -am -DskipTests -Dhttp.proxyHost=[your_ip] -Dhttp.proxyPort=[your_port] -Dhttps.proxyHost=[your_ip] -Dhttps.proxyPort=[your_port] -Dcheckstyle.skip=true -Dspotless.skip=true
```

After compilation, the CED-DB binary package will be generated in: `distribution/target`.

### Compile Other Modules

By adding `-P compile-cpp`, you can compile the C++ client API.

**Note:** The following directories need to be added to the source root to avoid compilation errors in the IDE:
`thrift/target/generated-sources/thrift`, `thrift-sync/target/generated-sources/thrift`, `thrift-cluster/target/generated-sources/thrift`, `thrift-influxdb/target/generated-sources/thrift`, and `antlr/target/generated-sources/antlr4`.

**For IntelliJ IDEA:** After compiling with the Maven command above, right-click the project name and select `Maven -> Reload project`.

### Configuration

The configuration files are located in the `conf` folder (`edge_conf` for the Edge version and `cloud_conf` for the Cloud version):
* Environment configuration module (`datanode-env.bat`, `datanode-env.sh`)
* System configuration module (`iotdb-datanode.properties`)
* Logging configuration module (`logback.xml`)

## Getting Started

You can test the installation by following the steps below. If no errors are returned, the installation is complete.

### Start CED-DB

You can start CED-DB by running the scripts in the `sbin` folder. The specific steps are as follows (Linux):

Start the Edge version:

Run ConfigNode-Edge:
```
sudo distribution/target/apache-iotdb-1.3.4-SNAPSHOT-confignode-bin/apache-iotdb-1.3.4-SNAPSHOT-confignode-bin/sbin/start-confignode.sh -c [your config]
```

Run DataNode-Edge:
```
sudo distribution/target/apache-iotdb-1.3.4-SNAPSHOT-datanode-bin/apache-iotdb-1.3.4-SNAPSHOT-server-bin/sbin/start-datanode.sh -c [your config]
```

Start the Cloud version:

Run ConfigNode-Cloud:
```
sudo distribution/target/apache-iotdb-1.3.4-SNAPSHOT-confignode-bin/apache-iotdb-1.3.4-SNAPSHOT-confignode-bin/sbin/start-confignode.sh -c [your config]
```

Run DataNode-Cloud:
```
sudo distribution/target/apache-iotdb-1.3.4-SNAPSHOT-datanode-bin/apache-iotdb-1.3.4-SNAPSHOT-server-bin/sbin/start-datanode.sh -c [your config]
```

### Use CED-DB

#### Use the CLI

CED-DB provides different ways to interact with the server. Here we introduce the basic steps for inserting and querying data using the CLI tool.

After installing CED-DB, there is a default user `root` whose default password is also `root`. Users can log in to the CLI and use CED-DB with this default account. The startup script for the CLI is the `start-cli` script in the `sbin` folder.
When executing the script, the user should specify the IP, port, `USER_NAME`, and password. The default parameters are `-h 127.0.0.1 -p 6667 -u root -pw root`.

The following is the command to start Cli-Edge:

```
> distribution/target/apache-iotdb-1.3.4-SNAPSHOT-cli-bin/apache-iotdb-1.3.4-SNAPSHOT-cli-bin/sbin/start-cli.sh
```

The following is the command to start Cli-Cloud:

```
> distribution/target/apache-iotdb-1.3.4-SNAPSHOT-cli-bin/apache-iotdb-1.3.4-SNAPSHOT-cli-bin/sbin/start-cli.sh
```

The command-line client is interactive, so if everything is ready, you should see the welcome banner and message:

```
  ______  ________ ______      ______   ______      
 .' ___  ||_   __  |_   _ `.   |_   _ `.|_   _ \    
/ .'   \_|  | |_ \_| | | `. \    | | `. \ | |_) |  
| |         |  _| _  | |  | |    | |  | | |  __'.  
\ `.___.'\ _| |__/ |_| |_.' /   _| |_.' /_| |__) | 
 `.____ .'|________|______.'   |______.'|_______/   
  version x.x.x

CED-DB> login successfully
CED-DB>
```

#### Basic Commands

Now let us introduce how to create timeseries, insert data, and query data.

CED-DB uses the same commands as IoTDB. Data in CED-DB is organized as timeseries. Each timeseries contains multiple `data-time` pairs and belongs to a database.
Before defining a timeseries, we should first create a database using `CREATE DATABASE`. Here is an example:

```
CED-DB> CREATE DATABASE root.ln
```

We can also use `SHOW DATABASES` to check the created databases:

```
CED-DB> SHOW DATABASES
+--------+
|Database|
+--------+
| root.ln|
+--------+
Total line number = 1
```

After setting the database, we can use `CREATE TIMESERIES` to create a new timeseries.
When creating a timeseries, we should define its data type and encoding scheme. Here we create two timeseries:

```
CED-DB> CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN
CED-DB> CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE
```

To query a specific timeseries, we can use `SHOW TIMESERIES <Path>`. `<Path>` indicates the path of the queried timeseries. The default value is `null`, which means querying all timeseries in the system (the same as `SHOW TIMESERIES root`).
Some examples are shown below:

1. Query all timeseries in the system:

```
CED-DB> SHOW TIMESERIES
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|                   timeseries|alias|database|dataType|encoding|compression|tags|attributes|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln.wf01.wt01.temperature| null|      root.ln|   FLOAT|     RLE|     SNAPPY|null|      null|
|     root.ln.wf01.wt01.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+-----------------------------+-----+-------------+--------+--------+-----------+----+----------+
Total line number = 2
```

2. Query a specified timeseries (`root.ln.wf01.wt01.status`):

```
CED-DB> SHOW TIMESERIES root.ln.wf01.wt01.status
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
|              timeseries|alias|database|dataType|encoding|compression|tags|attributes|
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln.wf01.wt01.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
Total line number = 1
```

Inserting timeseries data is a basic operation in CED-DB. You can use the `INSERT` command to do this.
Before insertion, you should specify the timestamp and the suffix path name:

```
CED-DB> INSERT INTO root.ln.wf01.wt01(timestamp,status) values(100,true);
CED-DB> INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values(200,false,20.71)
```

The data you just inserted will be displayed as follows:

```
CED-DB> SELECT status FROM root.ln.wf01.wt01
+-----------------------------+------------------------+
|                         Time|root.ln.wf01.wt01.status|
+-----------------------------+------------------------+
|1970-01-01T08:00:00.100+08:00|                    true|
|1970-01-01T08:00:00.200+08:00|                   false|
+-----------------------------+------------------------+
Total line number = 2
```

You can also query multiple timeseries with a single SQL statement:

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

If you need to modify the time zone in the CLI, you can use the following statements:

```
CED-DB> SET time_zone=+00:00
Time zone has set to +00:00
CED-DB> SHOW time_zone
Current time zone: Z
```

After that, query results will be displayed in the updated time zone:

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

You can exit using the following commands:

```
CED-DB> quit
or
CED-DB> exit
```

Since CED-DB uses the same commands as IoTDB, for more information about IoTDB SQL-supported commands, please refer to the [IoTDB User Guide](https://iotdb.apache.org/zh/UserGuide/Master/QuickStart/QuickStart.html).

### Stop CED-DB

The server can be stopped by pressing `ctrl-C` or by running the following script:

```
> distribution/target/apache-iotdb-1.3.4-SNAPSHOT-confignode-bin/apache-iotdb-1.3.4-SNAPSHOT-confignode-bin/sbin/stop-standalone.sh
```

