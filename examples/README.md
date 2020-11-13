# PyFlink 从入门到精通

[toc]

Flink 是目前非常火热的流处理框架，可以很好地实现批流一体，即一套代码即可以用于批处理，也可以用于流处理。

官方文档很好地解释了 Flink 的基础概念和工作原理，但是对于 Python 开发者而言少了很多例子，很难快速基于 PyFlink 进行开发。

本文档基于常见的业务场景，提供了一些案例，来帮助小伙伴们快速上手 PyFlink。

在上手之前，先大致介绍一下 PyFlink：
1. PyFlink 是 Flink 对 Java API 的一层封装，运行时会启动 JVM 来与 Python 进行通信。
2. Flink 提供了多种不同层级的 API，层级越高封装程度越高（建议在 SQL API 或 Table API 进行编程），层级由高到低分别为:
    1) SQL API
    2) Table API
    3) DataStream API / DataSet API
    4) Stateful Streaming Processing
3. Flink 在做批处理的时候，是将批数据当成特殊的流数据来处理。

本教程的建议使用步骤是：
1. 按顺序实践。
1. 阅读每个案例的代码，理解每个部分的作用。
1. 阅读每个案例的代码头部文档里提到的扩展阅读，加深理解。

## 1、批处理 Word Count

该案例展示了如何用 Flink 做批处理，统计指定文件下的单词数，并将统计结果写入到新的文件下。

运行命令为：

```bash
sh run.sh examples/1_word_count/batch.py
```

`batch.py` 批处理脚本执行逻辑是：
1. 首先，创建 Blink 批处理环境。（关于 Blink 和 Flink 的区别，见脚本 `1_word_count/batch.py` 的文件头）。
2. 然后，创建源表（source），这里使用 `filesystem` 作为连接器，按照指定的 `csv` 格式来批量地读取指定路径的文件（或文件夹）。
3. 接着，创建结果表（sink），这里同样使用 `filesystem` 连接器，以便将处理后结果写入目标文件（或文件夹）内。
4. 最后，编写批处理逻辑，完成批处理任务。

对于上述每个阶段的实现，`batch.py` 脚本提供了基于 Table API 和基于 SQL API 这两种不同方式的实现。

SQL API 是对 Table API 的更高层次的封装，内部实现了一些优化，便于阅读，但相对地功能没有 Table API 来得全面和强大，本质上并无不同之处，读者根据需求可自行选择实现方法。

运行后的结果写到了同级目录下的 result.csv 中：

```
flink,3
pyflink,2
```

通过本案例，可以学到：
1. 如何创建批处理环境。
2. 如何创建数据源表和结果表，实现有单一流入和单一流出的 dataflow 的处理。
3. 如何用 Table API 和 SQL API 实现聚合逻辑。

## 2、自定义函数 UDF

> **业务场景**
> 
> 通过 Flink 对系统上报的 syslog 进行实时解析并生成告警，搭建实时监控告警系统。

该案例是对线上实时监控告警的一小部分进行了改造（线上是流处理，本案例改成了批处理），展示了如何用 Flink 管理自定义函数 UDF，来实现复杂的日志解析逻辑。

同时，本案例也是 [官方文档](https://ci.apache.org/projects/flink/flink-docs-master/zh/dev/python/table-api-users-guide/udfs/python_udfs.html)
里的标量函数（ Scalar Function ）的一个简单实现，在 PyFlink 1.11 里的 UDF 已经比较强大了，更多技巧请前往官方文档进行学习。

运行命令为：

```bash
sh run.sh examples/2_udf/batch.py
```

运行后的结果写到了同级目录下的 result.csv 中：

```
syslog-user,172.25.0.2,10.200.80.1,root,root,ls,2020-10-28 14:27:28
syslog-user,172.25.0.2,10.200.80.1,root,root,ll,2020-10-28 14:27:31
syslog-user,172.25.0.2,10.200.80.1,root,root,vim /etc/my.cnf,2020-10-28 14:28:06
......
```

通过本案例，可以学到：
1. 如何创建并注册 UDF 。
2. 如何使用 UDF 。

## 3、实时 CDC

该案例展示了如何用 Flink 进行数据库的实时同步。

### 3.1、MySQL CDC

> **业务场景**
> 
> 监听 MySQL 的 binlog 数据变更，并实时同步到另一个 MySQL。

`CDC` 是 `change data capture`，即变化数据捕捉。CDC 是数据库进行备份的一种方式，常用于大量数据的备份工作。

CDC 分为入侵式的和非入侵式两种：
* 入侵式：如基于触发器备份、基于时间戳备份、基于快照备份。
* 非入侵式：如基于日志的备份。

MySQL 基于日志的 CDC 就是要开启 mysql 的 binlog（ binary log ）。
如果使用本教程的 docker-compose 安装的 MySQL 8.0.22，默认是开启 binlog 的。
实际业务 MySQL 可能未开启 binlog，可以通过执行下面的命令，查看 `log_bin` 变量的值是否为 `ON`。

```mysql
show variables like '%log_bin%';
```

`3_database_sync/stream.py` 实时同步脚本通过下面 3 个 Jar 包，实现了 MySQL CDC。
1. flink-connector-jdbc_2.11-1.11.2.jar：通过 JDBC 连接器来从数据库里读取或写入数据。
2. flink-sql-connector-mysql-cdc-1.1.0.jar：通过 MySQL-CDC 连接器从 MySQL 的 binlog 里提取更改。
3. mysql-connector-java-5.1.49.jar：JDBC 连接器的驱动（ 帮助 java 连接 MySQL ）。

需要注意的是：
1. 创建 source 表的时候，定义连接器为 mysql-cdc ，写法请参照 [文档](https://ci.apache.org/projects/flink/flink-docs-stable/zh/dev/table/connectors/jdbc.html) 。
1. 创建 sink 表的时候，定义连接器为 jdbc，写法请参照 [文档](https://github.com/ververica/flink-cdc-connectors) 。
1. 由于 source 表可能有更新或删除，因此只能使用 upsert 模式来实现实时同步，该模式要求 sink 表里设置主键（ primary key）。

运行命令为：

```bash
sh run.sh examples/3_database_sync/stream.py
```

通过本案例，可以学到：
1. 如何创建流处理环境。
2. 如何使用各类 connector ，以及如何管理在 pyflink 脚本中指定 jar 依赖。
3. 如何实现实时数仓的数据同步。

### 3.2、Kafka 同步到多数据源

> **业务场景**
> 
> 对于 kafka 里的 json 格式的线上日志，进行无状态的解析计算，并将结果同步到多个数据库 —— 如 Hive、MySQL。

> 待补充

通过本案例，可以学到：
1. 如何将数据同步到多个数据源。
1. 如何对实时同步进行优化。

## 4、有状态流处理

> **业务场景**
> 
> 考虑日志上下文，按不同用户 ID 分别统计各自每分钟的点击量，分别得到男女点击量各 TOP 10 的用户数据，并每 0.1 秒将更新结果到 Kafka。


该案例展示了如何用 Flink 进行有状态的流处理，通过窗口函数完成统计需求。
* 业务场景1：考虑日志上下文，按不同用户 ID 分别统计各自每分钟的点击量。

> 待补充

通过本案例，可以学到：
1. 如何使用窗口函数。

## 5、多流 join

该案例展示了如何用 Flink 对多个数据源进行 join 处理。

业务场景：在做 AI 算法的在线学习时，可能存在多个数据源，需要同时对多个数据源的数据进行处理并生成所需特征。

> 待补充

通过本案例，可以学到：
1. 如何指定多个 source 。
2. 进行多流 join。
