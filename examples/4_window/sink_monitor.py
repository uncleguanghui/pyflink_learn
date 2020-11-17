"""
读取 kafka 的用户操作数据并打印
"""
from kafka import KafkaConsumer
from reprint import output
import json

topic = 'click_rank'
bootstrap_servers = ['localhost:9092']
group_id = 'group7'

consumer = KafkaConsumer(
    topic,  # topic的名称
    group_id=group_id,  # 指定此消费者实例属于的组名，可以不指定
    bootstrap_servers=bootstrap_servers,  # 指定kafka服务器
    auto_offset_reset='latest',  # 'smallest': 'earliest', 'largest': 'latest'
)

with output(output_type="list", initial_len=22, interval=0) as output_lines:
    # 初始化打印行
    output_lines[0] = '=== 男 ==='
    output_lines[6] = '=== 女 ==='

    for msg in consumer:
        # 解析结果
        data = json.loads(msg.value)
        start_index = 1 if data['sex'] == '男' else 7
        rank = json.loads('[' + data['top10'] + ']')

        # 逐行打印
        for i in range(5):
            index = start_index + i
            if i < len(rank):
                name = list(rank[i].keys())[0]
                value = list(rank[i].values())[0]
                output_lines[index] = f'{name:6s} {value}'
            else:
                output_lines[index] = ''
