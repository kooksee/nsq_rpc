# -*- coding: utf-8 -*-
import inspect
import logging
import sys
import uuid
from os import listdir
from os.path import splitext

import msgpack
import nsq
import redis
from tornado import gen
from tornado import ioloop
from tornado.concurrent import Future
from tornado.ioloop import IOLoop

log = logging.getLogger(__name__)


class _Package(object):
    def __init__(self, req, _name, class_path, class_args):
        self.__req = req
        self.name = _name

        self.class_path = class_path
        self.class_args = class_args
        self.writer = nsq.Writer(['127.0.0.1:4150'])

    def f_m(self, conn, data):
        # print conn, data
        pass

    def call(self, _method, _m_args):
        _id = str(uuid.uuid4())
        self.__req[_id] = Future()

        self.writer.pub('mq_input', msgpack.packb((
            self.name,
            _id,
            self.class_path,
            self.class_args,
            _method,
            _m_args
        )), callback=self.f_m
                        )
        return self.__req[_id]


class RPCClient(object):
    __req = {}
    name = 'mq_output'
    code_path = 'test'

    def __init__(self):
        nsq.Reader(
            topic=self.name,
            channel='mq',
            name="mq_output.mq",
            # nsqd_tcp_addresses=['127.0.0.1:4150'],
            lookupd_http_addresses=['http://127.0.0.1:4161'],
            message_handler=self._handle_monitor,
            heartbeat_interval=10,
            output_buffer_size=4096,
            output_buffer_timeout=100,
            max_tries=5,
            max_in_flight=9,
            lookupd_poll_interval=60,
            low_rdy_idle_timeout=10,
            max_backoff_duration=128,
            lookupd_poll_jitter=0.3,
            lookupd_connect_timeout=1,
            lookupd_request_timeout=2,
        )

        import test
        self.code_path = test.__path__[0]
        print self.code_path

        self.redis_client = redis.Redis(host='127.0.0.1', port=6379)

        _p = self.code_path.split("/").pop()
        print(_p)

        for name, code in self.upload_code():
            self.redis_client.hset("__code__." + _p, name, code)

    def _handle_monitor(self, msg):
        _id, _res = msgpack.unpackb(msg.body, use_list=False)
        print _id, _res
        try:
            self.__req[_id].set_result(_res)
        except:
            pass
        return True

    def upload_code(self):
        code_path = self.code_path

        sys.path.append(code_path)

        for f in listdir(code_path):
            name, f_ext = splitext(f)

            if name.startswith('__') or name.endswith("__") or f_ext != '.py':
                continue

            __obj = __import__(name)
            for k, v in inspect.getmembers(__obj):
                if v.__class__.__name__ == 'type':
                    yield "{}.{}".format(name, k), "# -*- coding: utf-8 -*-\n\n\n{}".format(inspect.getsource(v))

    def package(self, class_path, args=None):
        return _Package(self.__req, self.name, class_path, args)


@gen.coroutine
def _call():
    _a = []
    for i in xrange(10):
        a = yield p.call('__call__', "jj")
        _a.append(a)
    raise gen.Return(_a)


@gen.coroutine
def _call1():
    _a = yield _call()
    print _a

    for i in xrange(10):
        b = yield s.call("get", 'http://www.baidu.com')
        print b


if __name__ == '__main__':
    c = RPCClient()
    p = c.package("test#hello.Hello", ("ssss", dict(www=2)))
    s = c.package("test#spider.Spider")

    ioloop.PeriodicCallback(_call1, 1000).start()

    loop = IOLoop.instance()
    loop.start()
