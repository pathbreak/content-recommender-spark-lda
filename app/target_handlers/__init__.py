from __future__ import print_function

class TargetHandlers(object):
    def __init__(self):
        self.handlers = {}
        
    def conf_init(self, app_conf):
        self.app_conf = app_conf

    def register(self, prototype_handler):
        self.handlers[prototype_handler.type] = prototype_handler
        
    def create_handler_instance(self, handler_type):
        handler_type = type(self.handlers[handler_type])
        return handler_type()


target_handlers = TargetHandlers()
