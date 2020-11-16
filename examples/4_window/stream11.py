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
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.window import Slide

kafka_servers = "localhost:9092"
kafka_consumer_group_id = "group8"  # group ID
source_topic = "user_action"  # 源数据
sink_topic = "click_rank"  # 结果

# ########################### 初始化流处理环境 ###########################

# 创建 Blink 流处理环境
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
t_env = StreamTableEnvironment.create(environment_settings=env_settings)
# 设置该参数以使用 UDF
t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

# ########################### 指定 jar 依赖 ###########################
# flink-sql-connector-kafka_2.11-1.11.2.jar：从扩展阅读 2 里获得，作用是通过 JDBC 连接器来从数据库里读取或写入数据。

dir_kafka_sql_connect = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                     'flink-sql-connector-kafka_2.11-1.11.2.jar')
t_env.get_config().get_configuration().set_string("pipeline.jars", 'file://' + dir_kafka_sql_connect)

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

# 创建长度为 60 秒的滑动窗口，每隔 1 秒滑动一次
slide_window = Slide.over("60.seconds").every("1.seconds").on('ts').alias("w")

# 基于 Table API 生成聚合后的表
table1 = t_env.from_path('source') \
    .filter("action = 'click'") \
    .filter("is_delete = 1") \
    .window(slide_window) \
    .group_by("w, sex, name") \
    .select("w.start AS start_time, "
            "w.end AS end_time, "
            "sex, "
            "name, "
            "count(1) AS cnt")

t_env.create_temporary_view('temp1', table1)

# 按性别分组，按点击量排序，得到男女各10个结果
topN_query = """
SELECT 
    sex,
    start_time,
    end_time,
    CONCAT('{"name":"', name, '","cnt":', CAST(cnt AS STRING), '}') as json_data
FROM
    (
    SELECT 
        start_time,
        end_time,
        sex,
        name,
        cnt,
        ROW_NUMBER() OVER (PARTITION BY sex ORDER BY cnt desc) AS rownum
    FROM
        temp1
    ) t
WHERE 
    rownum <= 10
"""
t_env.create_temporary_view('temp2', t_env.sql_query(topN_query))

# 合并到一起
combine_query = """
SELECT
    sex,
    LISTAGG(json_data) AS top10,
    start_time,
    end_time
    --FIRST(start_time) AS start_time,
    --FIRST(end_time) AS end_time
    --FIRST_VALUE(start_time) AS start_time,
    --FIRST_VALUE(end_time) AS end_time
FROM
    temp2
GROUP BY
    sex,
    start_time,
    end_time
"""
t_env.create_temporary_view('temp3', t_env.sql_query(combine_query))

t_env.from_path('temp3').execute_insert("sink")
# t_env.sql_query(combine_query).select('sex, top10, start_time, end_time').execute_insert("sink")

# t_env.from_path('temp2') \
#     .group_by('sex, start_time, end_time')\
#     .select('sex, '
#             'start_time, '
#             'end_time, '
#             'LISTAGG(json_data) AS top10') \
#     .execute_insert("sink")
t_env.execute(source_topic)
