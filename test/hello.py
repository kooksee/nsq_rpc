# -*- coding: utf-8 -*-


class Hello(object):
    def __init__(self, *args, **kwargs):
        print args, kwargs

    def __call__(self, *args, **kwargs):
        print self.__class__
        Hello1 = self.import_("test#hello.Hello1")
        print Hello1("dd", j=3)()

        return "hello world"


class Hello1(object):
    def __init__(self, *args, **kwargs):
        print args, kwargs

    def __call__(self, *args, **kwargs):
        print self.__class__
        return "hello + world"
