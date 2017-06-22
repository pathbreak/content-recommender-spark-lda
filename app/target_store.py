from __future__ import print_function
import datetime
import json
import os
import os.path

class TargetStore(object):
    '''
    Responsible for storing whatever text is fetched by the target handlers.
    
    As of now, this just saves all items directly to filesystem with date on which
    target is fetched. 
    
    Each entry is saved in its own JSON file containing the target metadata and fetched contents.
    
        # The directory structure for target storage as of now is:
        # TARGET_DIR
        #   /<datetime>/
        #           <handler-name>-<id1>.json
        #           <handler-name>-<id2>.json
        #               ...

    Future enhancements:
    - save the text in a directory tree so that system can get content only for a range
      of dates
    - save the text in a database
            
    '''
    def __init__(self, app_conf):
        self.app_conf = app_conf
        
        
    def prepare_to_store(self, handler_name):
        
        store_path = self.get_store_path(handler_name)
        
        if not os.path.exists(store_path):
            os.makedirs(store_path, exist_ok=True)
            if not os.path.exists(store_path):
                raise RuntimeError('Unable to create directory %s to store target content' % (store_path))
                
        return store_path
        
        
    def store_content(self, handler_name, entries, store_path=None):
        '''
        Every target handler calls this method to store contents of its
        fetched URLs.
        
        entries: 
            a list with single or multiple target entries. Each entry dict
            should have an 'id' and 'contents' key.
            
        store_path:
            Since store path involves a date which may change between calls, 
            it's preferable for caller to
            send a store path that's already been created once using prepare_to_store()
            
        '''
        
        if not store_path:
            store_path = self.get_store_path(handler_name)
            
        for e in entries:
            if e.get('contents', None) is None:
                e['contents'] = ''
                
            entry_filename = self.get_entry_filename(store_path, e)
            with open(entry_filename, 'w') as entry_file:
                # Caution: Don't set indent and separators or do any pretty printing to file,
                # because Spark is unable to handle a JSON file that spans multiple lines.
                json.dump(e, entry_file)
      
    
    
    def get_store_path(self, handler_name):
        subdir = os.path.join(
            self.app_conf['TARGET_DIR'], 
            datetime.datetime.now().strftime('%Y-%m-%d'))
            
        return subdir
            
            
    def already_stored(self, handler_name, entry, store_path=None):
        '''
        Method used by handlers to check if content is already stored before fetching.
        
        store_path:
            Since store path involves a date which may change between calls, 
            it's preferable for caller to
            send a store path that's already been created once using prepare_to_store()
        '''
        if not store_path:
            store_path = self.get_store_path(handler_name)
            
        entry_filename = self.get_entry_filename(store_path, entry)
        return os.path.exists(entry_filename)
        
        
        
    def get_entry_filename(self, store_path, entry):
        entry_filename = os.path.join(store_path, entry['id'] + '.json')
        return entry_filename
