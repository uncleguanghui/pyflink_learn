"""
基于 Flink 实现有状态的流处理

扩展阅读：
https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/ops/config.html
"""

import os
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.udf import ScalarFunction, udf

kafka_servers = "localhost:9092"
kafka_consumer_group_id = "group0"  # group ID
source_topic = "handwritten_digit"  # 源数据
sink_topic = "digit_predict"  # 结果

# ########################### 初始化流处理环境 ###########################
# 更多配置的设置请参考扩展阅读 1

# 创建 Blink 流处理环境，注意此处需要指定 StreamExecutionEnvironment，否则无法导入 java 函数
env = StreamExecutionEnvironment.get_execution_environment()
env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)
# 设置该参数以使用 UDF
t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

# ########################### 指定 jar 依赖 ###########################

dir_kafka_sql_connect = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                     'flink-sql-connector-kafka_2.11-1.11.2.jar')
t_env.get_config().get_configuration().set_string("pipeline.jars", 'file://' + dir_kafka_sql_connect)

# ########################### 指定 python 依赖 ###########################
# 可以在当前目录下看到 requirements.txt 依赖文件。
# 运行下述命令，成包含有安装包的 cached_dir 文件夹
# pip download -d cached_dir -r requirements.txt --no-binary :all:

dir_requirements = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'requirements.txt')
dir_cache = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'cached_dir')
if os.path.exists(dir_requirements):
    if os.path.exists(dir_cache):
        # 方式 1：指定包含依赖项的安装包的目录，它将被上传到集群以支持离线安装
        # 路径可以是绝对路径或相对路径，但注意路径前面不需要 file://
        t_env.set_python_requirements(dir_requirements, dir_cache)
    else:
        # 方式 2：指定描述依赖的依赖文件 requirements.txt，作业运行时下载，不推荐。
        t_env.set_python_requirements(dir_requirements)


# ########################### 注册 UDF ###########################

class Model(ScalarFunction):
    def __init__(self):
        # 加载模型
        self.model_name = 'online_ml_model'
        self.redis_params = dict(host='localhost', password='redis_password', port=6379, db=0)
        self.clf = self.load_model()

        self.interval_dump_seconds = 30  # 模型保存间隔时间为 30 秒
        self.last_dump_time = datetime.now()  # 上一次模型保存时间

        # y 的定义域
        self.classes = list(range(10))

        # 自定义的 4 类指标，用于评估模型和样本，指标值将暴露给外部系统以便于实时监控模型的状况
        self.metric_counter = None  # 从作业开始至今的所有样本数量
        self.metric_predict_acc = 0  # 模型预测的准确率（用过去 10 条样本来评估）
        self.metric_distribution_y = None  # 标签 y 的分布
        self.metric_total_10_sec = None  # 过去 10 秒内训练过的样本数量
        self.metric_right_10_sec = None  # 过去 10 秒内的预测正确的样本数

    def open(self, function_context):
        """
        访问指标系统，并注册指标，以便于在 webui (localhost:8081) 实时查看算法的运行情况。
        :param function_context:
        :return:
        """
        # 访问指标系统，并定义 Metric Group 名称为 online_ml 以便于在 webui 查找
        # Metric Group + Metric Name 是 Metric 的唯一标识
        metric_group = function_context.get_metric_group().add_group("online_ml")

        # 目前 PyFlink 1.11.2 版本支持 4 种指标：计数器 Counters，量表 Gauges，分布 Distribution 和仪表 Meters 。
        # 目前这些指标都只能是整数

        # 1、计数器 Counter，用于计算某个东西出现的次数，可以通过 inc()/inc(n:int) 或 dec()/dec(n:int) 来增加或减少值
        self.metric_counter = metric_group.counter('sample_count')  # 训练过的样本数量

        # 2、量表 Gauge，用于根据业务计算指标，可以比较灵活地使用
        # 目前 pyflink 只支持 Gauge 为整数值
        metric_group.gauge("prediction_acc", lambda: int(self.metric_predict_acc * 100))

        # 3、分布 Distribution，用于报告某个值的分布信息（总和，计数，最小，最大和平均值）的指标，可以通过 update(n: int) 来更新值
        # 目前 pyflink 只支持 Distribution 为整数值
        self.metric_distribution_y = metric_group.distribution("metric_distribution_y")

        # 4、仪表 Meters，用于汇报平均吞吐量，可以通过 mark_event(n: int) 函数来更新事件数。
        # 统计过去 10 秒内的样本量、预测正确的样本量
        self.metric_total_10_sec = metric_group.meter("total_10_sec", time_span_in_seconds=10)
        self.metric_right_10_sec = metric_group.meter("right_10_sec", time_span_in_seconds=10)

    def eval(self, x, y):
        """
        模型训练
        :param x: 图像的一维灰度数据，8*8=64 个值
        :param y: 图像的真实标签数据，0~9
        :return:
        """
        # 在线学习，即逐条训练模型
        # 需要把一维数据转成二维的，即在 x 和 y 外层再加个列表
        self.clf.partial_fit([x], [y], classes=self.classes)
        self.dump_model()  # 保存模型到 redis

        # 预测当前
        y_pred = self.clf.predict([x])[0]

        # 更新指标
        self.metric_counter.inc(1)  # 训练过的样本数量 + 1
        self.metric_total_10_sec.mark_event(1)  # 更新仪表 Meter ：来一条数据就 + 1 ，统计 10 秒内的样本量
        if y_pred == y:
            self.metric_right_10_sec.mark_event(1)  # 更新仪表 Meter ：来一条数据就 + 1 ，统计 10 秒内的样本量
        self.metric_predict_acc = self.metric_right_10_sec.get_count() / self.metric_total_10_sec.get_count()  # 准确率
        self.metric_distribution_y.update(y)  # 更新分布 Distribution ：训练过的样本数量 + 1

        # 返回预测结果
        return y_pred

    def load_model(self):
        """
        加载模型，如果 redis 里存在模型，则优先从 redis 加载，否则初始化一个新模型
        :return:
        """
        import redis
        import pickle
        import logging
        from sklearn.linear_model import SGDClassifier

        r = redis.StrictRedis(**self.redis_params)
        clf = None

        try:
            clf = pickle.loads(r.get(self.model_name))
        except TypeError:
            logging.info('Redis 内没有指定名称的模型，因此初始化一个新模型')
        except (redis.exceptions.RedisError, TypeError, Exception):
            logging.warning('Redis 出现异常，因此初始化一个新模型')
        finally:
            clf = clf or SGDClassifier(alpha=0.01, loss='log', penalty='l1')

        return clf

    def dump_model(self):
        """
        当距离上次尝试保存模型的时刻过了指定的时间间隔，则保存模型
        :return:
        """
        import pickle
        import redis
        import logging

        if (datetime.now() - self.last_dump_time).seconds >= self.interval_dump_seconds:
            r = redis.StrictRedis(**self.redis_params)

            try:
                r.set(self.model_name, pickle.dumps(self.clf, protocol=pickle.HIGHEST_PROTOCOL))
            except (redis.exceptions.RedisError, TypeError, Exception):
                logging.warning('无法连接 Redis 以存储模型数据')

            self.last_dump_time = datetime.now()  # 无论是否更新成功，都更新保存时间


model = udf(Model(), input_types=[DataTypes.ARRAY(DataTypes.INT()), DataTypes.TINYINT()],
            result_type=DataTypes.TINYINT())
t_env.register_function('train_and_predict', model)

# ########################### 创建源表(source) ###########################
# 使用 Kafka-SQL 连接器从 Kafka 实时消费数据。

t_env.execute_sql(f"""
CREATE TABLE source (
    x ARRAY<INT>,            -- 图片灰度数据
    actual_y TINYINT,            -- 实际数字
    ts TIMESTAMP(3)              -- 图片产生时间
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
    x ARRAY<INT>,              -- 图片灰度数据
    actual_y TINYINT,              -- 实际数字
    predict_y TINYINT              -- 预测数字
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

# 在线学习
t_env.sql_query("""
SELECT
    x,
    actual_y,
    train_and_predict(x, actual_y) AS predict_y
FROM
    source
""").insert_into("sink")
t_env.execute('Classifier Model Train')
