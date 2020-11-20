"""
删除 Redis 里指定的 key，或者清空 Redis 里所有的数据
"""
import redis
import sys

# 连接 Redis
redis_params = dict(
    host='localhost',
    password='redis_password',
    port=6379,
    db=0
)
r = redis.StrictRedis(**redis_params)

try:
    model = r.ping()
except (redis.exceptions.RedisError, TypeError, Exception) as err:
    raise Exception(f'无法连接 Redis：{err}')

# 如果没有传入任何 key ，则清空整个库
if len(sys.argv) == 1:
    r.flushdb()
else:
    # 如果传入了 key ，则检查是否存在，并删除
    for key in sys.argv[1:]:
        if r.exists(key):
            r.delete(key)
            print(f'已删除 {key}')
        else:
            print(f'{key} 不存在')
