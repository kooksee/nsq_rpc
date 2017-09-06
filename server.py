# -*- coding: utf-8 -*-
import logging
import ssl

import msgpack
import nsq
from tornado.ioloop import IOLoop

log = logging.getLogger(__name__)

import redis


class RPCServer(object):
    def __init__(self):
        self.__code_obj = {}

        nsq.Reader(
            topic='mq_input',
            channel='mq',
            name="mq_input.mq",
            nsqd_tcp_addresses=['127.0.0.1:4150'],
            lookupd_http_addresses=['http://127.0.0.1:4161'],
            message_handler=self._handle_monitor,
            heartbeat_interval=10,
            tls_options={'cert_reqs': ssl.CERT_NONE},
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

        self.writer = nsq.Writer(['127.0.0.1:4150'])
        self.redis_client = redis.Redis(host='127.0.0.1', port=6379)

    def __package(self, name):

        c = self.__code_obj.get(name)
        if c:
            return c

        redis_client = self.redis_client

        if '#' in name:
            _p, _f = name.split("#")
            _p = "." + _p
        else:
            _p = ""
            _f = name
        __s = redis_client.hget("__code__" + _p, _f)
        self.__code_obj[name] = self.__e(_f.split(".")[-1], __s)
        return self.__code_obj[name]

    def import_(self, name):
        return self.__package(name)

    def __e(self, name, source):
        try:
            exec source
            __c = eval(name)
            setattr(__c, "import_", self.import_)
            return __c
        except Exception, e:
            log.error(e.message)
            return e.message

    def f_m(self, conn, data):
        print conn, data

    def _handle_monitor(self, msg):
        _pp = msgpack.unpackb(msg.body, use_list=False)
        print _pp
        _name, _id, _package, _p_params, _method, _m_params = _pp

        _o = self.__code_obj.get((_package, str(_p_params)))
        if not _o:
            res = self.import_(_package)
            print res
            _o = res(_p_params)
            self.__code_obj[(_package, str(_p_params))] = _o

        __o = getattr(_o, _method)

        _r = __o(_m_params)
        print _r
        self.writer.pub(_name, msgpack.packb((_id, _r)), callback=self.f_m)
        return True


if __name__ == '__main__':
    RPCServer()
    loop = IOLoop.instance()
    loop.start()
