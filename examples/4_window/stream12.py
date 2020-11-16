"""
基于 Flink 的窗口函数 Tumble 和用户自定义聚合函数 UDAF，统计点击量最高的 10 个用户

扩展阅读
https://ci.apache.org/projects/flink/flink-docs-master/flinkDev/building.html#build-pyflink
https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/table/connectors/kafka.html
https://ci.apache.org/projects/flink/flink-docs-master/dev/python/table-api-users-guide/udfs/python_udfs.html
https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/dev/event_timestamp_extractors.html
"""

import os
from pyflink.table import DataTypes
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.window import Slide
from pyflink.table.udf import udaf

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

jars = []
for file in os.listdir(os.path.abspath(os.path.dirname(__file__))):
    if file.endswith('.jar'):
        jars.append(os.path.abspath(file))
str_jars = ';'.join(['file://' + jar for jar in jars])
t_env.get_config().get_configuration().set_string("pipeline.jars", str_jars)

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
    male_top10 STRING,          -- 点击量最高的 10 个男性用户
    female_top10 STRING,        -- 点击量最高的 10 个女性用户
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


# ########################### 注册 Pandas UDAF ###########################
# UDAF 是指用户定义的聚合函数，将多行的标量值映射到新的标量值。
# 为了使用 UDAF，需要将 PyFlink 升级到 1.12，请参考扩展阅读 1。
# 通用的 UDAF 仅支持 streaming mode 下的 blink planner 的 GroupBy 聚合，但还不支持 batch mode 或 windows 聚合。
# 为了实现 batch mode 或 windows 聚合，需要使用向量化的 Python 聚合函数（ Pandas UDAF ），即在使用 UDAF 时，指定参数 func_type="pandas"
# Pandas UDAF 将一个或多个 pandas.Series 作为输入，并返回一个标量值作为输出。
# 关于 Pandas UDAF 的使用，请参考扩展阅读 3。

@udaf(result_type=DataTypes.STRING(), func_type="pandas")
def male_click_top10(name, sex):
    """
    统计点击量最多的 10 个男人（只统计 sex=male、action=click 的数量，忽略 is_delete=1 的数据）
    :param name:
    :param sex:
    :param action:
    :param is_delete:
    :return:
    """
    names = name[sex == 'male']
    return names.value_counts().iloc[:10].to_json()


@udaf(result_type=DataTypes.STRING(), func_type="pandas")
def female_click_top10(name, sex, action, is_delete):
    """
    统计点击量最多的 10 个女人（只统计 sex=female、action=click 的数量，忽略 is_delete=1 的数据）
    :param name:
    :param sex:
    :param action:
    :param is_delete:
    :return:
    """
    names = name[sex == 'female']
    return names.value_counts().iloc[:10].to_json()


# 注册 udaf
t_env.create_temporary_function('male_click_top10', male_click_top10)
t_env.create_temporary_function('female_click_top10', female_click_top10)

# ########################### 流处理任务 ###########################

slide_window = Slide.over("60.seconds").every("1.seconds").on('ts').alias("w")  # 滑动

# 基于 Table API
t_env.from_path('source') \
    .filter("action = 'click' and is_delete = 1 ") \
    .window(slide_window) \
    .group_by("w") \
    .select("male_click_top10(name, sex) AS male_top10, "
            "female_click_top10(name, sex) AS female_top10, "
            "w.start AS start_time, "
            "w.end AS end_time") \
    .execute_insert("sink")
t_env.execute(source_topic)
