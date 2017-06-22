from __future__ import print_function

class HistoryHandlers(object):
    def __init__(self):
        self.handlers = []
        
    def conf_init(self, app_conf):
        self.app_conf = app_conf

    def register(self, handler):
        self.handlers.append(handler)
        handler.conf_init(self.app_conf)

history_handlers = HistoryHandlers()
