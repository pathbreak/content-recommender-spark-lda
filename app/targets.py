from __future__ import print_function
import os
import importlib

import target_handlers
from target_handlers import target_handlers as handlers

class TargetsProcessor(object):
    '''
    Fetches user configured target URLs using their respective handlers,
    and stores their content.
    '''
    def __init__(self, app_conf):
        self.app_conf = app_conf
        self.load_handlers()
        
        # At this point, TargetHandlers (__init__.py) has list of prototype 
        # handler instances. 
        # Now create handler instances according to configured TARGETS.
        self.handler_instances = []
        for target_conf in self.app_conf['TARGETS']:
            h_instance = handlers.create_handler_instance(target_conf['type'])
            self.handler_instances.append(h_instance)
            h_instance.conf_init(self.app_conf, target_conf)


    def load_handlers(self):
        # The handler plugins loaded here register themselves with target_handlers.TargetHandlers.
        plugin_files = os.listdir('target_handlers')
        for plugin_file in plugin_files:
            if not plugin_file.startswith('__'):
                importlib.import_module('target_handlers.' + plugin_file[0:-3])


                
    def fetch(self, target_store):
        for handler in self.handler_instances:
            handler.fetch(target_store)
            

        
