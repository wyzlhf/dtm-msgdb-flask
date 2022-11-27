import pymysql
from pymysql.cursors import Cursor
from typing import Dict, Union, Any, Tuple
from dtmcli import barrier, tcc, utils, saga, msg
from flask import Flask, request

app = Flask(__name__)
dbconf = {'host': '124.222.54.172', 'port': '3306', 'user': 'root', 'password': 'lhf19820130'}


def conn_new() -> Cursor:
    print('正在连接数据库：', dbconf)
    return pymysql.connect(host=dbconf['host'], user=dbconf['user'], password=dbconf['password'], database='').cursor()


def barrier_from_req(request: request):
    print('调用barrier_from_req()函数')
    return barrier.BranchBarrier(request.args.get('trans_type'), request.args.get('gid'), request.args.get('branch_id'),
                                 request.args.get('op'))


# 这是dtm服务地址
dtm: str = "http://localhost:36789/api/dtmsvr"
# 这是业务微服务地址
svc: str = "http://localhost:5000/api"

out_uid: int = 1
in_uid: int = 2


@app.get('/api/fireMsgdb')
def fire_msgdb() -> Dict[str, str]:
    print('调用fire_msgdb()函数，调用路径：/api/fireMsgdb，调用方式：get')
    req: Dict[str, int] = {'amount': 30}
    m: msg.Msg = msg.Msg(dtm, utils.gen_gid(dtm))
    m.add(req, svc + '/TransInSaga')

    def busi_callback(c):
        saga_adjust_balance(c, out_uid, -30)

    with barrier.AutoCursor(conn_new()) as cursor:
        m.do_and_submit_db(svc + '/queryprepared', cursor, busi_callback)
    return {'gid': m.trans_base.gid}


def saga_adjust_balance(cursor, uid: int, amount: int) -> None:
    print('调用saga_adjust_balance()函数')
    affected: Any = utils.sqlexec(
        cursor,
        "update dtm_busi.user_account set balance=balance+%d where user_id=%d and balance >= -%d" % (
            amount, uid, amount)
    )
    if affected == 0:
        raise Exception("update error, balance not enough")


@app.post('/api/TransInSaga')
def trans_in_saga():
    print('调用trans_in_saga()函数，调用路径：/api/TransInSaga，调用方式：post')
    with barrier.AutoCursor(conn_new()) as cursor:
        def busi_callback(c):
            print('调用busi_callback()函数，上层调用函数：trans_in_saga')
            saga_adjust_balance(c, in_uid, 30)

        barrier_from_req(request).call(cursor, busi_callback)
    return {"dtm_result": "SUCCESS"}


if __name__ == '__main__':
    app.run()
