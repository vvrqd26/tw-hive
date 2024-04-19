from flask import Flask, request, jsonify
from functools import wraps
from hive import Hive
from ant import Ant

app = Flask(__name__)
hive = Hive()
hive.run()


def get_idle_ant(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        ant = hive.get_idle_ant()
        ant.client.get_tweet_comments
        if not ant == None:
            kwargs['ant'] = ant
            print('use ant:', ant.name)
            res = f(*args, **kwargs)
            hive.return_ant(ant)
            return res
        else:
            return {'error': 'no idle ant'}
    return wrapper


# 调用推特
@app.route('/twitter_call/<method>', methods=['GET'])
@get_idle_ant
def getTweeter(method, **kwargs):
    ant: Ant = kwargs.pop('ant', None)
    args = request.args.to_dict()
    method_call = getattr(ant.client, method)
    res = method_call(**args)
    return jsonify(res)


# 推特迭代

@app.route('/twitter_iter/<method>', methods=['get'])
@get_idle_ant
def getTweeterIter(method, **kwargs):
    ant: Ant = kwargs.pop('ant', None)
    args = request.args.to_dict()
    method_call = getattr(ant.client, method)
    list = method_call(**args)
    res = []
    for item in list:
        res.append(item)
    return jsonify(res)


def __main__():
    app.run(host='0.0.0.0', port=3124)


if __name__ == '__main__':
    __main__()
