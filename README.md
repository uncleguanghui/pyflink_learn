# PyFlink 学习教程

基于 PyFlink 的学习文档，通过一个个小实践，便于小伙伴们快速入手 PyFlink

[toc]

## 1、环境搭建

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

启动后，运行 `docker ps` 可以看到起了 3 个容器，如下所示

```bash
CONTAINER ID        IMAGE                       COMMAND                  CREATED             STATUS              PORTS                                                NAMES
e79edea7db13        wurstmeister/kafka          "start-kafka.sh"         5 seconds ago       Up 3 seconds        0.0.0.0:9092->9092/tcp                               kafka
fb620e98215e        sheepkiller/kafka-manager   "./start-kafka-manag…"   5 seconds ago       Up 3 seconds        0.0.0.0:9001->9000/tcp                               kafka-manager
fb13d0a09845        wurstmeister/zookeeper      "/bin/sh -c '/usr/sb…"   5 seconds ago       Up 3 seconds        22/tcp, 2888/tcp, 3888/tcp, 0.0.0.0:2181->2181/tcp   zookeeper
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