"""
使用 PyFlink 进行无状态的流处理，实现 MySQL 数仓的实时同步。

拓展阅读：
https://ci.apache.org/projects/flink/flink-docs-stable/zh/dev/table/connectors/jdbc.html
https://github.com/ververica/flink-cdc-connectors
"""

import os
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# ########################### 初始化流处理环境 ###########################

# 创建 Blink 流处理环境
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
t_env = StreamTableEnvironment.create(environment_settings=env_settings)

# ########################### 指定 jar 依赖 ###########################
# flink-connector-jdbc_2.11-1.11.2.jar：从扩展阅读 1 里获得，作用是通过 JDBC 连接器来从数据库里读取或写入数据。
# flink-sql-connector-mysql-cdc-1.1.0.jar：从扩展阅读 2 里获得，作用是通过 MySQL-CDC 连接器从 MySQL 的 binlog 里提取更改。
# mysql-connector-java-5.1.49.jar：从扩展阅读 1 里获得，是 JDBC 连接器的驱动（ 帮助 java 连接 MySQL ）。

jars = []
for file in os.listdir(os.path.abspath(os.path.dirname(__file__))):
    if file.endswith('.jar'):
        jars.append(os.path.abspath(file))
str_jars = ';'.join(['file://' + jar for jar in jars])
t_env.get_config().get_configuration().set_string("pipeline.jars", str_jars)

# ########################### 创建源表(source) ###########################
# 使用 MySQL-CDC 连接器从 MySQL 的 binlog 里提取更改。
# 该连接器非官方连接器，写法请参照扩展阅读 2。

t_env.execute_sql(f"""
    CREATE TABLE source (
        id INT,              -- ID
        name STRING          -- 姓名
    ) WITH (
        'connector' = 'mysql-cdc',
        'hostname' = '127.0.0.1',
        'port' = '3306',
        'database-name' = 'flink',
        'table-name' = 'case3',
        'username' = 'root',
        'password' = 'root'
    )
""")

# 查看数据
# t_env.from_path('source').print_schema()  # 查看 schema
# t_env.from_path('source').to_pandas()  # 转为 pandas.DataFrame

# ########################### 创建结果表(sink) ###########################
# 将数据写入外部数据库时，Flink 使用 DDL 中定义的主键。如果定义了主键，则连接器将在 upsert 模式下运行，否则，连接器将在追加模式下运行。
# 在 upsert 模式下，Flink 将根据主键插入新行或更新现有行，Flink 可以通过这种方式确保幂等性。
# 在追加模式下，Flink 将所有记录解释为 INSERT 消息，如果在基础数据库中发生主键或唯一约束冲突，则 INSERT 操作可能会失败。

# 基于 SQL API
t_env.execute_sql(f"""
    CREATE TABLE sink (
        id INT,                            -- ID
        name STRING,                       -- 姓名
        PRIMARY KEY (id) NOT ENFORCED      -- 需要定义主键，让连接器在 upsert 模式下运行
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:mysql://127.0.0.1:3307/flink',
        'driver' = 'com.mysql.cj.jdbc.Driver',
        'table-name' = 'case3',
        'username' = 'root',
        'password' = 'root'
    )
""")

# ########################### 批处理任务 ###########################

# 基于 SQL API

# 方式1
t_env.from_path('source').insert_into('sink')

# 方式2
# t_env.sql_query("""
#     SELECT id,
#            name
#     FROM source
# """).insert_into('sink')

t_env.execute('sync by binlog')
