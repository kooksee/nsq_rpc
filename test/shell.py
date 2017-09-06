# -*- coding: utf-8 -*-

class Cmd(object):
    """
    执行简单的shell命令
    """

    def __init__(self, shell, *args, **kwargs):
        self.__shell = shell

    def __call__(self, *args, **kwargs):
        import os
        os.system(self.__shell)
        return "ok"

    def ls(self, args):
        import os
        os.system("ls {}".format(args))
        return "ok"
