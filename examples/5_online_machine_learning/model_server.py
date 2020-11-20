"""
从 Redis 里加载实时模型，提供预测服务
"""
import numpy as np
import redis
import pickle
import logging
import base64
from flask_cors import CORS
from flask import request, Flask, jsonify, render_template
from PIL import Image
from svglib.svglib import svg2rlg
from reportlab.graphics import renderPM

# 设置
redis_params = dict(
    host='localhost',
    password='redis_password',
    port=6379,
    db=0
)
model_key = 'online_ml_model'

# 创建 app
app = Flask(__name__)
CORS(app)

# 模型
clf = None


def load_latest_clf_model():
    """
    加载最新模型
    :return:
    """
    # 连接 Redis
    r = redis.StrictRedis(**redis_params)

    model = None
    try:
        model = pickle.loads(r.get(model_key))
    except TypeError:
        logging.exception('Redis 内没有找到模型，请确认 Key 值')
    except (redis.exceptions.RedisError, TypeError, Exception) as err:
        logging.exception(f'Redis 出现异常：{err}')

    return model


def format_svg_base64(s: str) -> np.array:
    """
    格式化 svg 格式的 base64 图片字符串为模型需要的 8 * 8 的灰度数组，并返回展平成 1 维的数据
    :param s: svg 格式的 base64 图片字符串
    :return: np.array
    """
    # base64 to svg
    with open('digit.svg', 'wb') as f:
        f.write(base64.b64decode(s))

    # svg to png
    drawing = svg2rlg("digit.svg")
    renderPM.drawToFile(drawing, "digit.png", fmt="PNG")

    # 由于 png 的长宽并不规则，因此需要先将 png 压缩到能装入目标大小的尺寸
    target_w, target_h = 8, 8  # 目标宽度和高度
    png = Image.open('digit.png')
    w, h = png.size  # 压缩前的宽和高
    scale = min(target_w / w, target_h / h)  # 确定压缩比例，保证缩小后到能装入 8 * 8
    new_w, new_h = int(w * scale), int(h * scale)  # 压缩后的宽和高
    png = png.resize((new_w, new_h), Image.BILINEAR)  # 压缩

    # 将 png 复制粘贴到目标大小的空白底图中间，并用白色填充周围
    new_png = Image.new('RGB', (target_w, target_h), (255, 255, 255))  # 新建空白底图
    new_png.paste(png, ((target_w - new_w) // 2, (target_h - new_h) // 2))  # 复制粘贴到空白底图的中间

    # 颜色反转（将手写的白底黑字，转变为模型需要的黑底白字），然后压缩数值到 0~16 的范围，并修改尺寸为 1 * 64
    array = 255 - np.array(new_png.convert('L'))  # 反转颜色
    array = (array / 255) * 16  # 将数值大小压缩到 0~16
    array = array.reshape(1, -1)  # 修改尺寸为 1 * 64

    return array


@app.route('/')
def home():
    return render_template('web.html')


@app.route('/predict', methods=['POST'])
def predict():
    global clf
    img_string = request.form['imgStr']
    # 格式化 svg base64 字符串为模型需要的数据
    data = format_svg_base64(img_string)
    # 每次都从 redis 里加载模型
    model = load_latest_clf_model()
    clf = model or clf  # 如果 redis 加载模型失败，就用最后一次加载的有效模型
    # 模型预测
    predict_y = int(clf.predict(data)[0])
    return jsonify({'success': True, 'predict_result': predict_y}), 201


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8066, debug=True)
