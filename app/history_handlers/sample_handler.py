from __future__ import print_function
import history_handlers

class SampleHandler(object):
    '''
    This is just a template for a history handler. 
    It shows all the methods a handler should implement, but otherwise does not
    actually do any processing. 
    '''
    
    def __init__(self):
        self.name = 'sample-handler'
        
        
    def conf_init(self, app_conf):
        '''
        Called by the handler registrar after this plugin registers itself.
        '''
        self.app_conf = app_conf

        
    def handle(self, entry, history_store):
        '''
        Handle an entry in the history file.
        If this handler does not intend to handle the URL, it should return False.
        
        entry: a dict of the form
            {
                "id": "14221",
                "lastVisitTime": "11/06/2017, 12:10:21",
                "lastVisitTimeTimestamp": 1497163221108.42,
                "title": "Some title",
                "typedCount": 0,
                "url": "https://www.youtube.com?v=1",
                "scheme" : "https",
                "domain" : "www.youtube.com",
                "path" : "",
                "params" : "",
                "query" : "v=1",
                "fragment" : "",
                "visitCount": 1
            },
        
        '''
        return False
        
    def completed(self, history_store):
        '''
        Invoked just once by the HistoryProcessor once all the entries are handled.
        If a handler prefers to process URLs in batches, it can cache the URLs in handle()
        and process them here.
        '''
        pass

# ----------------------------------------------------------------------


# Module entry point
history_handlers.history_handlers.register(SampleHandler())

