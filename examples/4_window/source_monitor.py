"""
读取 kafka 的用户操作数据并打印
"""
from kafka import KafkaConsumer

topic = 'user_action'
bootstrap_servers = ['localhost:9092']
group_id = 'group7'

consumer = KafkaConsumer(
    topic,  # topic的名称
    group_id=group_id,  # 指定此消费者实例属于的组名，可以不指定
    bootstrap_servers=bootstrap_servers,  # 指定kafka服务器
    auto_offset_reset='latest',  # 'smallest': 'earliest', 'largest': 'latest'
)

for msg in consumer:
    print(msg.value.decode('utf-8').encode('utf-8').decode('unicode_escape'))
