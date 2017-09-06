# -*- coding: utf-8 -*-


class Spider(object):
    def __init__(self, args):
        import requests
        self.r = requests

    def get(self, url):
        return self.r.get(url).text
