# PyFlink 从入门到精通

基于 PyFlink 的学习文档，通过一个个小实践，便于小伙伴们快速入手 PyFlink

[toc]

## 1、环境搭建

### 1.1、Mac

首先本地的 java 版本需要是 8 或 11

```bash
java -version
# 可能会看到 java version "1.8.0_111"
```

然后使用 brew 安装 Flink

```bash
brew switch apache-flink 1.11.2
...
flink --version  # 查看版本
```

在 `/usr/local/Cellar/apache-flink/1.11.2/libexec/bin/start-cluster.sh` 路径下，启动 flink

```bash
sh start-cluster.sh
```

为了方便，可以修改本地的 `~/.bash_profile` 文件，插入下面的 3 行内容（注意修改版本）然后运行 `source ~/.bash_profile` 来激活修改。

```bash
alias start-flink='/usr/local/Cellar/apache-flink/1.11.2/libexec/bin/start-cluster.sh'
alias stop-flink='/usr/local/Cellar/apache-flink/1.11.2/libexec/bin/stop-cluster.sh'
alias flink='/usr/local/Cellar/apache-flink/1.11.2/libexec/bin/flink'
```

### 1.2、其他系统

请参考 [官方文档](https://ci.apache.org/projects/flink/flink-docs-stable/ops/deployment/local.html)

### 1.3、基于 docker

从开发角度来看，以最快的速度搭建起一个可以运行的环境最为重要。

基于如下的 3 个角度，解释了为何使用 Docker：
1. Docker 可以很好地实现开发环境和生产环境的一致性。
1. 使用 Docker 可以模拟多节点集群，使用docker-compose 工具，我们可以轻松的在单台开发机上启动多个 Kafka 容器、zookeeper 容器，非常方便的实现了对分布式环境的模拟。
1. Docker 的安装、启动非常迅速。

首先，安装 [docker](https://www.docker.com/) 。

然后，启动 docker 编排服务：

```bash
# windows 系统再加下面这句
# set COMPOSE_CONVERT_WINDOWS_PATHS=1
docker-compose up -d
```

启动后，运行 `docker ps` 可以看到起了 4 个容器，如下所示

```bash
CONTAINER ID        IMAGE                           COMMAND                  CREATED             STATUS              PORTS                                                  NAMES
591cbb84c3a3        pyflink/playgrounds:1.11.0      "/docker-entrypoint.…"   12 seconds ago      Up 11 seconds       6121-6123/tcp, 8081/tcp                                taskmanager
6d97f2ff7a1f        pyflink/playgrounds:1.11.0      "/docker-entrypoint.…"   13 seconds ago      Up 12 seconds       6123/tcp, 8081/tcp, 0.0.0.0:8088->8088/tcp             jobmanager
2ad7bbf3ec9a        wurstmeister/kafka:2.13-2.6.0   "start-kafka.sh"         13 seconds ago      Up 12 seconds       0.0.0.0:9092->9092/tcp                                 kafka
a21fbfd833f1        zookeeper:3.6.2                 "/docker-entrypoint.…"   13 seconds ago      Up 12 seconds       2888/tcp, 3888/tcp, 0.0.0.0:2181->2181/tcp, 8080/tcp   zookeeper
```

很简单地，我们完成了环境的搭建。

另外，停止命令如下：

```bash
# 停止
docker-compose stop

# 停止并删除
docker-compose down
```

## 2、运行

对于 `examples` 目录下的 pyflink 脚本，有两种方式来运行：

如果使用 docker 来搭建环境，则可以：

```bash
sh run.sh examples/xxx/xxx.py
```

如果没有使用 docker，而是直接本地安装了 pyflink，那么：

```bash
python examples/1_word_count/batch.py
```

如果本地已经安装了 Flink 则可以直接以 standalone 的模式来运行

```
flink run -m localhost:8081 -py stream11.py
```

具体教程见 [PyFlink 从入门到精通](examples/README.md)
