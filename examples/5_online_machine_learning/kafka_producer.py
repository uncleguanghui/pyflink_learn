"""
往 Kafka 里依次写入 sklearn 的 digits 手写数字图片数据集
"""
import numpy as np
from json import dumps
from time import sleep
from sklearn import datasets
from datetime import datetime
from kafka import KafkaProducer

# ######################### 设置 #########################
max_msg_per_second = 10  # 每秒钟的最大图片数，数据集共有 1797 张，预估 3 分钟左右可以传输完数据。
topic = "handwritten_digit"  # kafka topic
bootstrap_servers = ['localhost:9092']


def write_data():
    # 导入数据
    digits = datasets.load_digits()
    all_x = digits.data.astype(int)
    all_y = digits.target.astype(int)

    # 初始化生产者
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    while True:

        # 打乱数据
        idx = np.arange(digits.data.shape[0])
        np.random.shuffle(idx)
        all_x = all_x[idx]
        all_y = all_y[idx]

        for x, y in zip(all_x, all_y):
            # 生产数据，并发送到 kafka
            cur_data = {
                "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "x": x.tolist(),
                "actual_y": int(y)
            }
            producer.send(topic, value=cur_data)

            sleep(1 / max_msg_per_second)


write_data()
