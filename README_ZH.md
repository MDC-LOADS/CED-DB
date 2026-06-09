
[English](./README.md) | [中文](./README_ZH.md)

# CED-DB
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Version](https://maven-badges.herokuapp.com/maven-central/org.apache.iotdb/iotdb-parent/badge.svg)](http://search.maven.org/#search|gav|1|g:"org.apache.iotdb")
![](https://img.shields.io/badge/java--language-1.8%20%7C%2011%20%7C%2017-blue.svg)


# 简介
CED-DB(Cloud-Edge-Device DataBase)是一款云边端协同时序数据库管理系统，可以为用户提供数据收集、存储和查询等任务。CED-DB由于可以实现云服务器、边缘设备和传感器的三端协同，因此可以在满足工业 IoT 领域中海量数据处理、存储和复杂数据查询分析的需求的同时，还可以将查询任务转移，分散边缘设备的查询压力。

# 主要特点

CED-DB的主要特点如下：

1.分层架构与协同计算。CED-DB的云端提供高性能查询能力、高级分析与机器学习能力；边端负责本地数据预处理、初步分析、数据筛选与压缩，减少云端计算与存储压力；端侧直接从设备采集时序数据，具备低延迟处理能力，可实现本地存储。
2.灵活的查询与分析能力。CED-DB在边端查询负载过高时，会将查询任务发送至云端无缝进行查询，并会在边端负载恢复时重新切换回边端，实现了查询的灵活切换。
3.与先进的开放源码生态系统的无缝集成。CED-DB与IoTDB同源，在集成全部IoTDB全部功能的同时，支持LOADS数据库网页版Demo。
4.学习成本非常低。使用IoTDB原生语言，支持类似sql的语言、JDBC标准API和易于使用的导入/导出工具。
<!-- TOC -->

## 目录
- [CED-DB](#ced-db)
- [简介](#简介)
- [主要特点](#主要特点)
  - [目录](#目录)
- [快速开始](#快速开始)
  - [环境准备](#环境准备)
  - [安装](#安装)
    - [从源码构建](#从源码构建)
      - [配置](#配置)
  - [开始](#开始)
    - [启动 CED-DB](#启动-ced-db)
    - [使用 CED-DB](#使用-ced-db)
      - [使用 Cli 命令行](#使用-cli-命令行)
      - [基本的命令](#基本的命令)
    - [停止 CED-DB](#停止-ced-db)
- [联系我们](#联系我们)
- [声明](#声明)




<!-- /TOC -->


# 快速开始

这篇简短的指南将带您了解使用CED-DB的基本过程。如需更详细的介绍，请联系我们。

## 环境准备
要使用CED-DB，您需要:
1. Java >= 1.8 (目前 11 到 17 已经被验证可用，推荐使用15。请确保环变量境路径已正确设置)。
2. Maven >= 3.6 。
3. 设置 max open files 为 65535，以避免"too many open files"错误。
4. （可选） 将 somaxconn 设置为 65535 以避免系统在高负载时出现 "connection reset" 错误。
    ```
    # Linux
    > sudo sysctl -w net.core.somaxconn=65535
   
    # FreeBSD or Darwin
    > sudo sysctl -w kern.ipc.somaxconn=65535
    ```

## 安装

在这篇《快速入门》中，我们简要介绍如何使用源代码安装CED-DB。

## 从源码构建

### 关于准备Thrift编译器

如果您使用Windows，请跳过此段。

我们使用Thrift作为RPC模块来提供客户端-服务器间的通信和协议支持，因此在编译阶段我们需要使用Thrift 0.13.0
（或更高）编译器生成对应的Java代码。 Thrift只提供了Windows下的二进制编译器，Unix下需要通过源码自行编译。

如果你有安装权限，可以通过`apt install`, `yum install`, `brew install`来安装thrift编译器，然后在下面的编译命令中
都添加如下参数即可：`-Dthrift.download-url=http://apache.org/licenses/LICENSE-2.0.txt -Dthrift.exec.absolute.path=<你的thrift可执行文件路径>`。


同时我们预先编译了一个Thrift编译器，并将其上传到了GitHub ，借助一个Maven插件，在编译时可以自动将其下载。（例如您在Linux系统下编译时，可忽略此段。）
该预编译的Thrift编译器在gcc8，Ubuntu, CentOS, MacOS下可以工作，但是在更低的gcc
版本以及其他操作系统上尚未确认。
如果您发现因为网络问题总是提示下载不到thrift文件，那么您需要手动下载，并将编译器放置到目录`{project_root}\thrift\target\tools\thrift_0.12.0_0.13.0_linux.exe`。
如果您放到其他地方，就需要在运行maven的命令中添加：`-Dthrift.download-url=http://apache.org/licenses/LICENSE-2.0.txt -Dthrift.exec.absolute.path=<你的thrift可执行文件路径>`。

如果您对Maven足够熟悉，您也可以直接修改我们的根pom文件来避免每次编译都使用上述参数。
Thrift官方网址为：https://thrift.apache.org/

### 代码准备

从 git 克隆源代码:
```
https://github.com/MDC-LOADS/Cloud-Edge-Device-DataBase.git
```
默认的主分支是Edge分支，如果你想使用Cloud版本，请切换 tag:
```
git checkout CED-DB-Cloud
```
如果想使用Edge版本，请切换tag：
```
git checkout CED-DB-Edge
```
如果您想使用分布式环境，需要将下面的代码进行修改：
# Edge版
在`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/execution/operator/source/AbstractSeriesAggregationScanOperator.java`,`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/execution/operator/source/SeriesScanOperator.java`,`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/service/IOMonitor.java`,`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/execution/ServiceImpl.java`中将`localhost`改为`cloud_ip`，端口号改为`10740`。
在`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/execution/ServerStart.java`中将`localhost`改为`0.0.0.0`。
在`/conf_edge/iotdb-confignode.properties`中配置confignode：
```
cn_internal_address=edge_ip
cn_internal_port=10710
cn_consensus_port=10720
cn_seed_config_node=edge_ip:10710
```
在`/conf_edge/iotdb-datanode.properties`中配置datanode：
```
dn_rpc_address=0.0.0.0
dn_rpc_port=6667
n_internal_address=0.0.0.0
dn_internal_port=10730
dn_mpp_data_exchange_port=10740
dn_schema_region_consensus_port=10750
dn_data_region_consensus_port=10760
dn_seed_config_node=edge_ip:10710
```
# Cloud版
在`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/execution/operator/source/AbstractSeriesAggregationScanOperator.java`,`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/execution/operator/source/SeriesScanOperator.java`,`iotdb-core\datanode\src\main\java\org\apache\iotdb\db\queryengine\execution\operator\process\FilterAndProjectOperator.java`中将`localhost`改为`cloud_ip`，端口号改为`10740`。
在`iotdb-core\datanode\src\main\java\org\apache\iotdb\db\queryengine\plan\planner\OperatorTreeGenerator.java`中将`ackSend()`中的`localhost`改为`cloud_ip`，端口号改为`10740`。
在`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/execution/ServerStart.java`中将`localhost`改为`0.0.0.0`。
在`/conf_cloud/iotdb-confignode.properties`中配置confignode：
```
cn_internal_address=cloud_ip
cn_internal_port=10710
cn_consensus_port=10720
cn_seed_config_node=cloud_ip:10710
```
在`/conf_cloud/iotdb-datanode.properties`中配置datanode：
```
dn_rpc_address=0.0.0.0
dn_rpc_port=6667
n_internal_address=0.0.0.0
dn_internal_port=10730
dn_mpp_data_exchange_port=10740
dn_schema_region_consensus_port=10750
dn_data_region_consensus_port=10760
dn_seed_config_node=cloud_ip:10710
```
### 源码编译 CED-DB

在 Cloud-Edge-Device-DateBase 根目录下执行:

```
> mvn clean package -pl distribution -am -DskipTests -Dcheckstyle.skip=true
```

当您需要使用代理时，可以执行下述命令：

```
> mvn -T 32 clean package -pl distribution -am -DskipTests -Dhttp.proxyHost=[your_ip] -Dhttp.proxyPort=[your_port] -Dhttps.proxyHost=[your_ip] -Dhttps.proxyPort=[your_port] -Dcheckstyle.skip=true
```

编译完成后, CED-DB 二进制包将生成在: "distribution/target".


### 编译其他模块

通过添加 `-P compile-cpp` 可以进行c++客户端API的编译。

**注意："`thrift/target/generated-sources/thrift`"， "`thrift-sync/target/generated-sources/thrift`"，"`thrift-cluster/target/generated-sources/thrift`"，"`thrift-influxdb/target/generated-sources/thrift`" 和  "`antlr/target/generated-sources/antlr4`" 目录需要添加到源代码根中，以免在 IDE 中产生编译错误。**

**IDEA的操作方法：在上述maven命令编译好后，右键项目名称，选择"`Maven->Reload project`"，即可。**

### 配置

配置文件在"conf"文件夹下(Edge版本为edge_conf,Cloud版本为cloud_conf)
* 环境配置模块(`datanode-env.bat`, `datanode-env.sh`),
* 系统配置模块(`iotdb-datanode.properties`)
* 日志配置模块(`logback.xml`)。

## 开始

您可以通过以下步骤来测试安装，如果执行后没有返回错误，安装就完成了。

### 启动 CED-DB

可以通过运行sbin文件夹下的脚本启动CED-DB，具体操作步骤如下(Linux系统)：

启动Edge版本：

需要在`distribution/target/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/sbin/start-confignode.sh`文件中`source "$(dirname "$0")/iotdb-common.sh"`的下方添加下列命令：

```
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5200"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DCONFIGNODE_CONF=./conf_edge"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -Dlogback.configurationFile=./conf_edge/logback-confignode.xml"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DTSFILE=./conf_edge"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -Dname=iotdb/.ConfigNodeEdge"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DIOTDB_CONF=./conf_edge"
```

在`distribution/target/apache-iotdb-1.3.0-SNAPSHOT-datanode-bin/apache-iotdb-1.3.0-SNAPSHOT-server-bin/sbin/start-datanode.sh`文件中`source "$(dirname "$0")/iotdb-common.sh"`的下方添加下列命令：

```
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5210"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DCONFIGNODE_CONF=./conf_edge"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -Dlogback.configurationFile=./conf_edge/logback-datanode.xml"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DTSFILE=./conf_edge"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -Dname=iotdb/.DataNodeEdge"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DIOTDB_CONF=./conf_edge"
```

运行ConfigNode-Edge：

```
sudo distribution/target/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/sbin/start-confignode.sh
```
运行DataNode-Edge：

```
sudo distribution/target/apache-iotdb-1.3.0-SNAPSHOT-datanode-bin/apache-iotdb-1.3.0-SNAPSHOT-server-bin/sbin/start-datanode.sh
```

启动Cloud版本：

需要在`distribution/target/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/sbin/start-confignode.sh`文件中`source "$(dirname "$0")/iotdb-common.sh"`的下方添加下列命令：

```
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5200"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DCONFIGNODE_CONF=./conf_cloud"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -Dlogback.configurationFile=./conf_cloud/logback-confignode.xml"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DTSFILE=./conf_cloud"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -Dname=iotdb/.ConfigNodeEdge"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DIOTDB_CONF=./conf_cloud"
```

在`distribution/target/apache-iotdb-1.3.0-SNAPSHOT-datanode-bin/apache-iotdb-1.3.0-SNAPSHOT-server-bin/sbin/start-datanode.sh`文件中`source "$(dirname "$0")/iotdb-common.sh"`的下方添加下列命令：

```
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5210"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DCONFIGNODE_CONF=./conf_cloud"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -Dlogback.configurationFile=./conf_cloud/logback-datanode.xml"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DTSFILE=./conf_cloud"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -Dname=iotdb/.DataNodeEdge"
IOTDB_JVM_OPTS="$IOTDB_JVM_OPTS -DIOTDB_CONF=./conf_cloud"
```

运行ConfigNode-Cloud：

```
sudo distribution/target/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/sbin/start-confignode.sh
```
运行DataNode-Cloud：

```
sudo distribution/target/apache-iotdb-1.3.0-SNAPSHOT-datanode-bin/apache-iotdb-1.3.0-SNAPSHOT-server-bin/sbin/start-datanode.sh
```
注意：如果您需要伪分布式环境，请将上述的边端端口号改为5201和5211.
### 使用 CED-DB

#### 使用 Cli 命令行

CED-DB提供了与服务器交互的不同方式，这里我们将介绍使用 Cli 工具插入和查询数据的基本步骤。

安装 CED-DB 后，有一个默认的用户`root`，它的默认密码也是`root`。用户可以使用这个
默认用户登录 Cli 并使用 CED-DB。Cli 的启动脚本是 sbin 文件夹中的 start-cli 脚本。
在执行脚本时，用户应该指定 IP，端口，USER_NAME 和 密码。默认参数为`-h 127.0.0.1 -p 6667 -u root -pw root`。


下面是启动 Cli-Edge 的命令:

```
> distribution/target/apache-iotdb-1.3.0-SNAPSHOT-cli-bin/apache-iotdb-1.3.0-SNAPSHOT-cli-bin/sbin/start-cli.sh -p 6668
```
下面是启动 Cli-Cloud 的命令:

```
> distribution/target/apache-iotdb-1.3.0-SNAPSHOT-cli-bin/apache-iotdb-1.3.0-SNAPSHOT-cli-bin/sbin/start-cli.sh -p 6667
```

命令行客户端是交互式的，所以如果一切就绪，您应该看到欢迎标志和声明:

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
#### 基本的命令

现在，让我们介绍创建 timeseries、插入数据和查询数据的方法。

CED-DB的命令与IoTDB的命令相同。CED-DB中的数据组织为 timeseries。每个 timeseries 包含多个`数据-时间`对，由一个 database 拥有。
在定义 timeseries 之前，我们应该先使用 CREATE DATABASE 来创建一个数据库，下面是一个例子:

```
CED-DB> CREATE DATABASE root.ln
```

我们也可以使用`SHOW DATABASES`来检查已创建的数据库:

```
CED-DB> SHOW DATABASES
+--------+
|Database|
+--------+
| root.ln|
+--------+
Total line number = 1
```

在设置 database 之后，我们可以使用CREATE TIMESERIES来创建一个新的TIMESERIES。
在创建 timeseries 时，我们应该定义它的数据类型和编码方案。这里我们创建两个 timeseries:


```
CED-DB> CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN
CED-DB> CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE
```

为了查询特定的timeseries，我们可以使用 `SHOW TIMESERIES <Path>`. <Path> 表示被查询的 timeseries 的路径. 默认值是`null`, 表示查询系统中所有的 timeseries (同`SHOW TIMESERIES root`).
以下是一些示例:

1. 查询系统中所有 timeseries:

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

2. 查询指定的 timeseries(root.ln.wf01.wt01.status):

```
CED-DB> SHOW TIMESERIES root.ln.wf01.wt01.status
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
|              timeseries|alias|database|dataType|encoding|compression|tags|attributes|
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
|root.ln.wf01.wt01.status| null|      root.ln| BOOLEAN|   PLAIN|     SNAPPY|null|      null|
+------------------------+-----+-------------+--------+--------+-----------+----+----------+
Total line number = 1
```

插入 timeseries 数据是CED-DB的一个基本操作，你可以使用`INSERT` 命令来完成这个操作。
在插入之前，您应该指定时间戳和后缀路径名:

```
CED-DB> INSERT INTO root.ln.wf01.wt01(timestamp,status) values(100,true);
CED-DB> INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values(200,false,20.71)
```

你刚才插入的数据会显示如下:

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

您还可以使用一条SQL语句查询多个 timeseries 数据:

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

如果需要修改 Cli 中的时区，您可以使用以下语句:

```
CED-DB> SET time_zone=+00:00
Time zone has set to +00:00
CED-DB> SHOW time_zone
Current time zone: Z
```

之后查询结果将会以更新后的新时区显示:

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

你可以使用如下命令退出:

```
CED-DB> quit
or
CED-DB> exit
```

由于CED-DB与IoTDB的指令相同，有关IoTDB SQL支持的命令的更多信息，请参见[IoTDB用户指南](https://iotdb.apache.org/zh/UserGuide/Master/QuickStart/QuickStart.html)。

### 停止 CED-DB

server 可以使用 "ctrl-C" 或者执行下面的脚本:

```
> distribution/target/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/apache-iotdb-1.3.0-SNAPSHOT-confignode-bin/sbin/stop-standalone.sh
```

