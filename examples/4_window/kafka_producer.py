"""
往 Kafka 里批量产生随机的用户操作数据
"""
import random
import numpy as np
from json import dumps
from time import sleep
from faker import Faker
from datetime import datetime
from kafka import KafkaProducer

# ######################### 设置 #########################
seed = 2020  # 设置随机数种子，保证每次运行的结果都一样
num_users = 50  # 50 个用户
max_msg_per_second = 20  # 每秒钟的最大消息数
run_seconds = 3600  # 脚本最长运行时间，防止无限写入 kafka
topic = "user_action"  # kafka topic
bootstrap_servers = ['localhost:9092']

fake = Faker(locale='zh_CN')
Faker.seed(seed)
random.seed(seed)


class UserGroup:
    def __init__(self):
        """
        为指定数量的用户分配不同的出现概率，每次按概率分布获取用户姓名
        """
        self.users = [self.gen_male() if random.random() < 0.6 else self.gen_female() for _ in range(num_users)]
        prob = np.cumsum(np.random.uniform(1, 100, num_users))  # 用户点击概率的累加
        self.prob = prob / prob.max()  # 压缩到 0 ~ 1

    @staticmethod
    def gen_male():
        """
        生成男人
        """
        return {'name': fake.name_male(), 'sex': '男'}

    @staticmethod
    def gen_female():
        """
        生成女人
        """
        return {'name': fake.name_female(), 'sex': '女'}

    def get_user(self):
        """
        随机获得用户
        """
        r = random.random()  # 生成一个 0~1的随机数
        index = np.searchsorted(self.prob, r)
        return self.users[index]


def write_data():
    group = UserGroup()
    start_time = datetime.now()

    # 初始化生产者
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    while True:
        now = datetime.now()

        # 生产数据，并发送到 kafka
        user = group.get_user()
        cur_data = {
            "ts": now.strftime("%Y-%m-%d %H:%M:%S"),
            "name": user['name'],
            "sex": user['sex'],
            "action": 'click' if random.random() < 0.9 else 'scroll',  # 用户的操作
            "is_delete": 0 if random.random() < 0.9 else 1  # 10% 的概率丢弃这条数据
        }
        producer.send(topic, value=cur_data)

        # 终止条件
        if (now - start_time).seconds > run_seconds:
            break

        # 停止时间
        sleep(1 / max_msg_per_second)


write_data()
