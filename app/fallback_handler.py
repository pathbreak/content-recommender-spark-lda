from __future__ import print_function

class FallbackHandler(object):
    def __init__(self):
        pass

    def conf_init(self, app_conf):
        self.app_conf = app_conf

    def handle(self, entry, history_store):
        #print("Fallback : ", entry['url'])
        return True
        
    def completed(self, history_store):
        pass
        
