from __future__ import print_function
import json
import importlib
import importlib.util
import os
from urllib.parse import urlparse
import sys
import traceback
from collections import Counter

import history_handlers
from history_handlers import history_handlers as handlers
from fallback_handler import FallbackHandler


class HistoryProcessor(object):
    '''
    Processes browsing history JSON files by handing over each URL to a matching
    handler.
    
    Any unhandled URL is sent to the fallback handler. By default, it does
    nothing. But if user instead wishes to simply fetch any remaining URL, it can
    be implemented there.
    
    As of now, this class does not do any kind of deduplication of history items.
    So even if an entry was in yesterday's history file and is in today's file too, it'll
    still handle the entry like it's a new one. 
    Changing this behavior by deduplicating entries and processing only new entries
    will be considered in the future.
    '''
    def __init__(self, app_conf):
        self.app_conf = app_conf
        handlers.conf_init(self.app_conf)
        self.load_handlers()

        self.fallback_handler = FallbackHandler()
        self.fallback_handler.conf_init(self.app_conf)
        
    def load_handlers(self):
        # The handler plugins loaded here register themselves with history_handlers.HistoryHandlers.
        plugin_files = os.listdir('history_handlers')
        for plugin_file in plugin_files:
            if not plugin_file.startswith('__'):
                importlib.import_module('history_handlers.' + plugin_file[0:-3])

        
    def process_history(self, filepath, history_store):
        
        # In future
        # - this should save the entire history file in datastore area 
        #   or store all entries in a database
        # - support HDFS paths
        
        with open(filepath, 'r') as history_file:
            history = json.load(history_file)
            
        domain_counts = Counter()
            
        for entry in history:
            urlparts = urlparse(entry['url'])
            entry['scheme'] = urlparts.scheme
            entry['domain'] = urlparts.netloc
            entry['path'] = urlparts.path
            entry['params'] = urlparts.params
            entry['query'] = urlparts.query
            entry['fragment'] = urlparts.fragment
            
            domain_counts.update({entry['domain'] : 1})
            
            handled = False
            try:
                for handler in handlers.handlers:
                    if handler.handle(entry, history_store):
                        handled = True
                        break
                
                if not handled:
                    self.fallback_handler.handle(entry, history_store)
            except:
                # No need to stop all processing if one URL fails.
                print('\n\n\nERROR: Could not process history entry %s\n\tReason:%s\n\n\n' % (
                    entry['url'], traceback.print_exc() ) )
            
        for handler in handlers.handlers:
            try:
                handler.completed(history_store)
            except:
                # No need to stop all processing if one URL fails.
                print('\n\n\nERROR: Could not complete processing all entries\n\tReason:%s\n\n\n' % (
                    traceback.print_exc() ) )
        
        try:
            self.fallback_handler.completed(history_store)
        except:
            # No need to stop all processing if one URL fails.
            print('\n\n\nERROR: Could not complete fallback processing\n\tReason:%s\n\n\n' % (
                traceback.print_exc() ) )
                
        print('\n\nBrowsing History domain counts:')
        for domain,count in domain_counts.most_common():
            print(domain, ':', count)

    
