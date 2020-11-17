"""
基于 Flink 的窗口函数 Slide ，每 1 秒统计过去 1 分钟内点击量最高的 10 个男性用户和 10 个女性用户

扩展阅读
https://ci.apache.org/projects/flink/flink-docs-master/flinkDev/building.html#build-pyflink
https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/connectors/kafka.html
https://ci.apache.org/projects/flink/flink-docs-master/dev/python/table-api-users-guide/udfs/python_udfs.html
https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/event_timestamp_extractors.html
https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/python/table-api-users-guide/udfs/vectorized_python_udfs.html
https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/functions/udfs.html
"""

import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.window import Slide

kafka_servers = "localhost:9092"
kafka_consumer_group_id = "group8"  # group ID
source_topic = "user_action"  # 源数据
sink_topic = "click_rank"  # 结果

# ########################### 初始化流处理环境 ###########################

# 创建 Blink 流处理环境，注意此处需要指定 StreamExecutionEnvironment，否则无法导入 java 函数
env = StreamExecutionEnvironment.get_execution_environment()
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)
# 设置该参数以使用 UDF
t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

# ########################### 指定 jar 依赖 ###########################
# flink-sql-connector-kafka_2.11-1.11.2.jar：从扩展阅读 2 里获得，作用是通过 JDBC 连接器来从数据库里读取或写入数据。
# flink-udf-1.0-SNAPSHOT.jar：包含了若干种基于 Java 开发的自定义 UDF 和 UDAF

dir_kafka_sql_connect = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                     'flink-sql-connector-kafka_2.11-1.11.2.jar')
t_env.get_config().get_configuration().set_string("pipeline.jars", 'file://' + dir_kafka_sql_connect)

dir_java_udf = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'flink-udf-1.0-SNAPSHOT.jar')
t_env.get_config().get_configuration().set_string("pipeline.classpaths", 'file://' + dir_java_udf)

# ########################### 注册 UDF ###########################
# 本次用到的是 flink-udf-1.0-SNAPSHOT.jar 里的 TopN 类，用于计算点击量最高的 N 个人

# 指定 Java 聚合函数的入口
t_env.register_java_function('getTopN', 'com.flink.udf.TopN')

# ########################### 创建源表(source) ###########################
# 使用 Kafka-SQL 连接器从 Kafka 实时消费数据。
# 使用方法请参考扩展阅读 2
# 声明 ts 是事件时间属性，并且用 延迟 5 秒的策略来生成 watermark
# 要求 kafka 数据里包含 ts 字段（不能为空）以代表事件时间
# 关于 watermark 的使用请参考扩展阅读 4

t_env.execute_sql(f"""
CREATE TABLE source (
    name VARCHAR,                -- 姓名
    sex VARCHAR,                 -- 性别
    action VARCHAR,              -- 操作
    is_delete BIGINT,            -- 是否要删除：0-不删除，1-删除
    ts TIMESTAMP(3),             -- 点击时间
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) with (
    'connector' = 'kafka',
    'topic' = '{source_topic}',
    'properties.bootstrap.servers' = '{kafka_servers}',
    'properties.group.id' = '{kafka_consumer_group_id}',
    'scan.startup.mode' = 'latest-offset',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true',
    'format' = 'json'
)
""")

# ########################### 创建结果表(sink) ###########################
# 将统计结果实时写入到 Kafka

t_env.execute_sql(f"""
CREATE TABLE sink (
    sex STRING,                 -- 性别
    top10 STRING,               -- 点击量最高的 10 个用户
    start_time TIMESTAMP(3),    -- 窗口开始时间
    end_time TIMESTAMP(3)       -- 窗口结束时间
) with (
    'connector' = 'kafka',
    'topic' = '{sink_topic}',
    'properties.bootstrap.servers' = '{kafka_servers}',
    'properties.group.id' = '{kafka_consumer_group_id}',
    'scan.startup.mode' = 'latest-offset',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true',
    'format' = 'json'
)
""")

# ########################### 流处理任务 ###########################

# 方法1：基于 SQL API 来聚合
# 在 GROUP BY 里使用 HOP 滑动窗口函数，入参分别为：时间戳、滑动步长、窗口长度
# 在 SELECT 里使用 HOP_START 和 HOP_END 来获得滑动窗口的开始时间和结束时间
t_env.sql_query("""
SELECT
    sex,
    getTopN(name, 10, 1) AS top10,
    HOP_START(ts, INTERVAL '1' SECOND, INTERVAL '60' SECOND) AS start_time,
    HOP_END(ts, INTERVAL '1' SECOND, INTERVAL '60' SECOND) AS end_time
FROM
    source
WHERE
    action = 'click'
    AND is_delete = 0
GROUP BY
    sex,
    HOP(ts, INTERVAL '1' SECOND, INTERVAL '60' SECOND)
""").insert_into("sink")
t_env.execute('Top10 User Click')

# 方法2：基于 Table API 来聚合
# 创建长度为 60 秒、滑动步长为 1 秒的滑动窗口 slide_window ，并重命名为 w
# 使用 w.start, w.end 来获得滑动窗口的开始时间与结束时间
# slide_window = Slide.over("60.seconds").every("1.seconds").on('ts').alias("w")
# t_env.from_path('source') \
#     .filter("action = 'click'") \
#     .filter("is_delete = 1") \
#     .window(slide_window) \
#     .group_by("w, sex") \
#     .select("sex, "
#             "getTopN(name, 10, 1) as top10, "
#             "w.start AS start_time, "
#             "w.end AS end_time") \
#     .insert_into("sink")
#
# t_env.execute('Top10 User Click')
