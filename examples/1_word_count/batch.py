"""
使用 PyFlink 进行批处理。

网上可查的教程往往基于 Table API 来实现 word count。
本教程同时还提供了基于更 high-level 的 SQL API 的实践。

Flink vs Blink：
两者的区别见拓展阅读 3，相对而言 Blink 在 SQL 方面的支持更强大，建议使用。
两者对两类 API 的支持如下：
---
           | Flink 批环境 |   Blink 批环境
  SQL API  |       X     |       √
TABLE API  |       √     |       √
---

拓展阅读：
https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/python/table-api-users-guide/intro_to_table_api.html
https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/python/table-api-users-guide/conversion_of_pandas.html
https://ci.apache.org/projects/flink/flink-docs-master/dev/table/common.html#main-differences-between-the-two-planners

"""

import os
import shutil
from pyflink.table import BatchTableEnvironment, EnvironmentSettings
from pyflink.table import DataTypes
from pyflink.table.descriptors import Schema, OldCsv, FileSystem

# ########################### 初始化批处理环境 ###########################

# 创建 Blink 批处理环境
env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
t_env = BatchTableEnvironment.create(environment_settings=env_settings)

# 创建 Flink 批处理环境
# env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_old_planner().build()
# t_env = BatchTableEnvironment.create(environment_settings=env_settings)

# ########################### 创建源表(source) ###########################
# source 指数据源，即待处理的数据流的源头，这里使用同级目录下的 word.csv，实际中可能来自于 MySQL、Kafka、Hive 等

dir_word = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'word.csv')

# 基于 SQL API
t_env.execute_sql(f"""
    CREATE TABLE source (
        id BIGINT,     -- ID
        word STRING    -- 单词
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file://{dir_word}',
        'format' = 'csv'
    )
""")

# 基于 Table API
# t_env.connect(FileSystem().path(dir_word)) \
#     .with_format(OldCsv()
#                  .field('id', DataTypes.BIGINT())
#                  .field('word', DataTypes.STRING())) \
#     .with_schema(Schema()
#                  .field('id', DataTypes.BIGINT())
#                  .field('word', DataTypes.STRING())) \
#     .create_temporary_table('source')

# 查看数据
# t_env.from_path('source').print_schema()  # 查看 schema
# t_env.from_path('source').to_pandas()  # 转为 pandas.DataFrame

# ########################### 创建结果表(sink) ###########################
# sink 指发送数据，即待处理的数据流的出口，这里使用同级目录下的 result.csv，实际中可能会把处理好的数据存到 MySQL、Kafka、Hive 等

dir_result = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'result')

# 如果文件/文件夹存在，则删除
if os.path.exists(dir_result):
    if os.path.isfile(dir_result):
        os.remove(dir_result)
    else:
        shutil.rmtree(dir_result, True)

# 基于 SQL API
t_env.execute_sql(f"""
    CREATE TABLE sink (
        word STRING,   -- 单词
        cnt BIGINT     -- 出现次数
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file://{dir_result}',
        'format' = 'csv'
    )
""")

# 基于 SQL API
# t_env.connect(FileSystem().path(dir_result)) \
#     .with_format(OldCsv()
#                  .field('word', DataTypes.STRING())
#                  .field('cnt', DataTypes.BIGINT())) \
#     .with_schema(Schema()
#                  .field('word', DataTypes.STRING())
#                  .field('cnt', DataTypes.BIGINT())) \
#     .create_temporary_table('sink')

# ########################### 批处理任务 ###########################

# 基于 SQL API
t_env.sql_query("""
    SELECT word
           , count(1) AS cnt
    FROM source
    GROUP BY word
""").insert_into('sink')
t_env.execute('t')

# 基于 table API
# t_env.from_path('source') \
#     .group_by('word') \
#     .select('word, count(1) AS cnt') \
#     .execute_insert('sink')
