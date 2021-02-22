"""
使用 PyFlink 进行批处理，运用 UDF 来实现复杂的计算逻辑。

拓展阅读：
https://ci.apache.org/projects/flink/flink-docs-master/zh/dev/python/table-api-users-guide/udfs/python_udfs.html
"""
import os
import shutil
from pyflink.table import EnvironmentSettings, DataTypes, BatchTableEnvironment
from pyflink.table.udf import udf
from pyflink.table.descriptors import OldCsv, Schema, FileSystem

# ########################### 初始化批处理环境 ###########################

# 创建 Flink 批处理环境
env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
t_env = BatchTableEnvironment.create(environment_settings=env_settings)
t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

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

# ########################### 创建源表(source) ###########################

dir_log = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'syslog.text')

# 基于 Table API 创建 source 表
# source 表只有 1 列，名为 line
t_env.connect(FileSystem().path(dir_log)) \
    .with_format(OldCsv()
                 .line_delimiter('\n')
                 .field('line', DataTypes.STRING())) \
    .with_schema(Schema()
                 .field('line', DataTypes.STRING())) \
    .create_temporary_table('source')

# ########################### 创建结果表(sink) ###########################

dir_result = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'result')

# 如果文件/文件夹存在，则删除
if os.path.exists(dir_result):
    if os.path.isfile(dir_result):
        os.remove(dir_result)
    else:
        shutil.rmtree(dir_result, True)

# 基于 Table API
t_env.connect(FileSystem().path(dir_result)) \
    .with_format(OldCsv()
                 .field('topic', DataTypes.STRING())
                 .field('fake_country', DataTypes.STRING())
                 .field('ip_src', DataTypes.STRING())
                 .field('ip_host', DataTypes.STRING())
                 .field('user_name', DataTypes.STRING())
                 .field('user_group', DataTypes.STRING())
                 .field('msg_content', DataTypes.STRING())
                 .field('msg_time', DataTypes.STRING())) \
    .with_schema(Schema()
                 .field('topic', DataTypes.STRING())
                 .field('fake_country', DataTypes.STRING())
                 .field('ip_src', DataTypes.STRING())
                 .field('ip_host', DataTypes.STRING())
                 .field('user_name', DataTypes.STRING())
                 .field('user_group', DataTypes.STRING())
                 .field('msg_content', DataTypes.STRING())
                 .field('msg_time', DataTypes.STRING())) \
    .create_temporary_table('sink')


# ########################### 注册 UDF ###########################

@udf(input_types=[], result_type=DataTypes.STRING())
def get_fake_country():
    # 随机生成国家
    import faker

    return faker.Faker(locale='zh_CN').country()


@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def get_topic(line):
    import re
    if 'IN=' in line and 'OUT=' in line and 'MAC=' in line:
        return 'syslog-iptables'
    elif '=======================================' in line or re.search(r'localhost (.+?): \[', line, re.M | re.I):
        return 'syslog-user'
    else:
        return 'syslog-system'


@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def get_ip_host(line):
    import re
    pattern_ip = r'(\d{1}|[1-9]{1}\d{1}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1}|[1-9]{1}\d{1}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1}' \
                 r'|[1-9]{1}\d{1}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1}|[1-9]{1}\d{1}|1\d\d|2[0-4]\d|25[0-5])'
    pattern_ip_host = f'ip:{pattern_ip}'

    ip_host = re.search(pattern_ip_host, line, re.M | re.I) or ''
    if ip_host:
        ip_host = ip_host.group()[3:]
    return ip_host


@udf(input_types=[DataTypes.STRING(), DataTypes.STRING()], result_type=DataTypes.STRING())
def get_ip_src(topic, line):
    import re
    pattern_ip = r'(\d{1}|[1-9]{1}\d{1}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1}|[1-9]{1}\d{1}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1}' \
                 r'|[1-9]{1}\d{1}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1}|[1-9]{1}\d{1}|1\d\d|2[0-4]\d|25[0-5])'
    pattern_time1 = '[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'  # YYYY-mm-dd HH:MM:SS
    pattern_ip_src = rf'{pattern_time1} (?:({pattern_ip})|(\({pattern_ip}\)))'

    if topic == 'syslog-user':
        ip_src = re.search(pattern_ip_src, line, re.M | re.I) or ''
        if ip_src:
            ip_src = ip_src.group().split(' ')[2]
    else:
        ip_src = ''
    return ip_src


@udf(input_types=[DataTypes.STRING(), DataTypes.STRING()], result_type=DataTypes.STRING())
def get_user_name(topic, line):
    import re
    pattern_ip = r'(\d{1}|[1-9]{1}\d{1}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1}|[1-9]{1}\d{1}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1}' \
                 r'|[1-9]{1}\d{1}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1}|[1-9]{1}\d{1}|1\d\d|2[0-4]\d|25[0-5])'
    pattern_time1 = '[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'  # YYYY-mm-dd HH:MM:SS
    pattern_char = '(.+?)'  # 字符
    pattern_user = f'{pattern_time1} {pattern_ip} {pattern_char} '  # 注意后面有个空格

    if topic == 'syslog-user':
        user_name = re.search(pattern_user, line, re.M | re.I) or ''
        if user_name:
            user_name = user_name.group().split(' ')[-2]
    else:
        user_name = ''
    return user_name


@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def get_user_group(line):
    import re
    pattern_char = '(.+?)'  # 字符
    pattern_group = f'localhost {pattern_char}:'

    user_group = re.search(pattern_group, line, re.M | re.I) or ''
    if user_group:
        user_group = user_group.group().split(' ')[-1][:-1]
    return user_group


@udf(input_types=[DataTypes.STRING(), DataTypes.STRING()], result_type=DataTypes.STRING())
def get_msg_content(topic, line):
    import re
    pattern_ip = r'(\d{1}|[1-9]{1}\d{1}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1}|[1-9]{1}\d{1}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1}' \
                 r'|[1-9]{1}\d{1}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1}|[1-9]{1}\d{1}|1\d\d|2[0-4]\d|25[0-5])'
    pattern_char = '(.+?)'  # 字符
    pattern_time1 = '[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'  # YYYY-mm-dd HH:MM:SS
    pattern_user = f'{pattern_time1} {pattern_ip} {pattern_char} '  # 注意后面有个空格
    pattern_content_login = f'======================================= is login'  # 用户登录信息
    pattern_content_user = f'{pattern_user}{pattern_char}"\'$'  # 用户命令信息，注意以 "' 2个字符结尾  # TODO 这里要改一下
    pattern_content_system = f'localhost {pattern_char}\'$'  # 系统命令信息，注意以 ' 1个字符结尾  # TODO 这里要改一下
    pattern_content_iptables = f'localhost(.*?)IN=(.*?)OUT=(.*?)MAC=(.*?)\'$'  # 防火墙命令信息，注意以 ' 1个字符结尾  # TODO 这里要改一下
    pattern_group = f'localhost {pattern_char}:'

    user_group = re.search(pattern_group, line, re.M | re.I) or ''
    if user_group:
        user_group = user_group.group().split(' ')[-1][:-1]

    if topic == 'syslog-user':
        msg_content = re.search(pattern_content_user, line, re.M | re.I) or ''
        if msg_content:
            msg_content = ' '.join(
                msg_content.group().split(' ')[4:])[:-2]  # 注意以 "' 2个字符结尾  # TODO 这里要改一下
        if not msg_content and pattern_content_login in line:
            msg_content = 'login'
    elif topic == 'syslog-system':
        msg_content = re.search(pattern_content_system, line, re.M | re.I) or ''
        if msg_content:
            msg_content = msg_content.group()[10:-1]  # 去掉 localhost 和空格，注意以 ' 1个字符结尾  # TODO 这里要改一下
            if msg_content.startswith(user_group):
                msg_content = msg_content[(len(user_group) + 2):]  # 去掉用户组，以及冒号和空格
    elif topic == 'syslog-iptables':
        msg_content = re.search(pattern_content_iptables, line, re.M | re.I) or ''
        if msg_content:
            msg_content = msg_content.group()[10:-1]  # 去掉 localhost 和空格，注意以 ' 1个字符结尾  # TODO 这里要改一下
            if msg_content.startswith(user_group):
                msg_content = msg_content[(len(user_group) + 2):]  # 去掉用户组，以及冒号和空格
    else:
        msg_content = ''
    return msg_content


@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def get_msg_time(line):
    import re
    from datetime import datetime

    pattern_time2 = '(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sept|Oct|Nov|Dec) ' \
                    '[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'  # Jun dd HH:MM:SS （少了年份）

    dict_month = {
        'Jan': '1',
        'Feb': '2',
        'Mar': '3',
        'Apr': '4',
        'May': '5',
        'Jun': '6',
        'Jul': '7',
        'Aug': '8',
        'Sept': '9',
        'Oct': '10',
        'Nov': '11',
        'Dec': '12'
    }

    msg_time = re.search(pattern_time2, line, re.M | re.I) or ''
    if msg_time:
        msg_time = msg_time.group()
        year = str(datetime.now().year)  # 没有年份的话用今年来代替
        month = dict_month[msg_time.split(" ")[0]]
        other = ' '.join(msg_time.split(" ")[1:])
        msg_time = f'{year}-{month}-{other}'  # 格式化为 YYYY-mm-dd
    return msg_time


# 注册 udf
t_env.register_function('get_topic', get_topic)
t_env.register_function('get_ip_src', get_ip_src)
t_env.register_function('get_ip_host', get_ip_host)
t_env.register_function('get_user_name', get_user_name)
t_env.register_function('get_user_group', get_user_group)
t_env.register_function('get_msg_content', get_msg_content)
t_env.register_function('get_msg_time', get_msg_time)
t_env.register_function('get_fake_country', get_fake_country)  # 获得虚假的国家

# ########################### 批处理任务 ###########################

# 基于 Table API
t_env.from_path('source') \
    .select('line, get_topic(line) AS topic') \
    .select('topic, '
            'get_fake_country() AS fake_country, '
            'get_ip_src(topic, line) AS ip_src, '
            'get_ip_host(line) AS ip_host, '
            'get_user_name(topic, line) AS user_name, '
            'get_user_group(line) AS user_group, '
            'get_msg_content(topic, line) AS msg_content, '
            'get_msg_time(line) AS msg_time') \
    .execute_insert('sink')
