from __future__ import print_function
import feedparser

import target_handlers

class FeedFetcher(object):
    '''
    A target handler for RSS/ATOM feeds.
    
    This uses just the feed's title, summary/description and tags as signals.
    It does not fetch or scrape the URLs in the feed.
    '''
    
    def __init__(self):
        self.type = 'feed'
        print('feed fetcher instance')


    def conf_init(self, app_conf, handler_conf):
        '''
        app_conf: the full application configuration, including all contents of conf.yml
        handler_conf: configuration attributes specific to this instance of the handler
        '''
        self.app_conf = app_conf
        self.handler_conf = handler_conf
        self.name = handler_conf.get('name', None)
        if not self.name:
            raise RuntimeError('conf.yml error: one of the feed TARGETS is missing name attribute')
            
        self.feed_url = handler_conf.get('url', None)
        if not self.feed_url:
            raise RuntimeError('conf.yml error: one of the feed TARGETS is missing url attribute')
        
        
    def fetch(self, target_store):
        print("Fetching ", self.feed_url)
        feed = feedparser.parse(self.feed_url)
        if not feed.entries:
            return
        
        entries = self.create_entries(feed.entries)
        
        # Store every fetched content along with its metadata in its own file.
        store_path = target_store.prepare_to_store(self.name)
        print('Store path:', store_path)
        target_store.store_content(self.name, entries, store_path=store_path)
            
        
    def create_entries(self, entries):
        target_entries = []
        id = 1

        for e in entries:
            title = e.get('title','')
            desc = e.get('description','')
            url = e['link']
            print(url)
            tags = e.get('tags', None)
            tags = ' '.join( [t['term'] for t in tags] ) if tags else ''
                
            # Since LDA is a bag of words model, things like newlines and order of terms don't matter.
            # Just combine all the attributes into a single string
            contents = ' '.join([title, desc, tags])
            
            entry = {
                'id': self.name + '-' + str(id),
                'url' : url,
                'title' : title,
                'details' : desc,
                'contents': contents
            }
            
            target_entries.append(entry)
            
            id += 1

        return target_entries
    
#-----------------------------------------------------------------------
# Module entry point
target_handlers.target_handlers.register(FeedFetcher())
