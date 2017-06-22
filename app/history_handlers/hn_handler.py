from __future__ import print_function
from bs4 import BeautifulSoup
import requests
from urllib.parse import parse_qs
import time

import history_handlers 

class HackerNewsHistoryHandler(object):
    '''
    Scrapes comments from https://news.ycombinator.com/item?id=<article> URLs.
    
    HN has an API, but since it involves multiple requests to fetch each comment,
    it seems simpler to just scrape a discussion URL. The DOM is also easy enough
    to process.
    '''
    def __init__(self):
        self.name = 'hn-history-handler'
        #self.entries_to_fetch = []
        self.store_path = None
        
    def conf_init(self, app_conf):
        self.app_conf = app_conf
        

    def handle(self, entry, history_store):
        if entry['domain'] == 'news.ycombinator.com':
            if entry['path'] == '/item':
                queries = parse_qs(entry['query'])
                item_id = queries.get('id', None)
                if item_id:
                    item_id = item_id[0]
                    print("HN plugin: Handling item id=", item_id)

                    if not self.store_path:
                        self.store_path = history_store.prepare_to_store(self.name)

                    #self.entries_to_fetch.append(entry)

                    if not history_store.already_stored(self.name, entry, self.store_path):
                        try:
                            self.fetch_contents(entry)
                            
                        except:
                            print('Error fetching: ', entry['url'])
                        else:
                            history_store.store_content(self.name, [entry], self.store_path)
                    
                    else:
                        print("HN plugin: Already stored, not fetching item id=", item_id)
                        
                    return True
                
        return False
        
        
    def fetch_contents(self, entry):
        # Comments are in <div class='comment'>.
        # Each such div has an unwanted <div class='reply'> which is simply deleted from DOM.
        resp = requests.get(entry['url'])
        resp.raise_for_status()
        
        soup = BeautifulSoup(resp.content, 'html.parser')
        for s in soup.find_all('div', attrs={'class':'reply'}):
            s.decompose()
            
        comments = soup.find_all('div', attrs={'class':'comment'})
        contents = ' '.join([s.get_text() for s in comments])
        
        entry['contents'] = contents


    def completed(self, history_store):
        #print("HN handler: Storing all entries metadata")
        #history_store.store_entries_metadata(self.name, self.entries_to_fetch, self.store_path)
        pass

# Module initialization
history_handlers.history_handlers.register(HackerNewsHistoryHandler())

