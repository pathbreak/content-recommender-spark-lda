from __future__ import print_function
from urllib.parse import parse_qs
from apiclient import discovery
import json
import os.path
import sys
import traceback

import history_handlers 

class YoutubeHistoryHandler(object):
    '''
    Uses Youtube v3 API to fetch video details such as titles, tags and descriptions.
    
    Each video's info has a structure like this: https://developers.google.com/youtube/v3/docs/videos#resource
    
    As of now, it does not fetch video captions or comments. 
    But those too are probably good signals for topic modeling. 
    Manually generated captions are probably a better signal than auto generated ones. 
    YT comments tend to be noisy with lots of memes and jokes and slang which 
    don't associate well with other signals like title or content of video. So comments
    are not used as of now.
    
    Design to minimize quota costs:
    -------------------------------
    The main query used by this handler is 'videos.list part=snippet' which entails  
    a quota cost of 3 units per query, regardless of how many ids are specified (see 
    https://developers.google.com/youtube/v3/docs/videos/list). So, to keep quota costs low,
    it's preferable to combine multiple video IDs into a single query rather than make
    one query per video ID.
    
    For supporting this, what this handler does is first cache all the video IDs, and do
    batch downloads of video details only on receiving the 'completed' notification from HistoryProcessor.
    According to https://stackoverflow.com/a/36371390,
    max number of IDs in a single request is 50.
    '''
    
    BATCH_REQUEST_SIZE = 50     # from https://stackoverflow.com/a/36371390
    API_KEY_FILE = 'yt_api_key.txt'
    
    def __init__(self):
        self.name = 'youtube-history-handler'
        self.entries_to_fetch = {}
        
        
    def conf_init(self, app_conf):
        self.app_conf = app_conf
        
        self.youtube_svc = self.get_youtube_service()
        

    def get_youtube_service(self):
        yt_api_key_file = os.path.join(self.app_conf['CONF_DIR'], self.API_KEY_FILE)
        if not os.path.exists(yt_api_key_file):
            raise RuntimeError("Error: YouTube API key file %s not found.\nObtain an API key as " + 
                "explained in https://developers.google.com/youtube/v3/getting-started#before-you-start.\n" + 
                "Then create a copy of conf/yt_api_key_template.yml as conf/yt_api_key.yml and insert the key in it." % (yt_api_key_file))
                
        with open(yt_api_key_file, 'r') as cred_file:
            creds = cred_file.read()
        
        service = discovery.build('youtube', 'v3', developerKey=creds)
        
        return service
        
        

    def handle(self, entry, history_store):
        if entry['domain'] == 'www.youtube.com':
            queries = parse_qs(entry['query'])
            video_id = queries.get('v', None)
            if video_id:
                video_id = video_id[0]
                print("Youtube plugin: Handling video id=", video_id)
                
                # Just cache the ID here. Video details are batch downloaded from
                # 'completed' callback in order to save quota costs.
                entry['video_id'] = video_id
                self.entries_to_fetch[video_id] = entry
                return True
            
        return False
        
        
    def completed(self, history_store):
        # TODO This handler does not implement any checks to see if video details have already been
        # stored before fetching details. This is just to avoid complexity for now, but it's 
        # recommended to add it.
        
        ids_to_fetch = list(self.entries_to_fetch.keys())
        batches = [ ids_to_fetch[x:x+self.BATCH_REQUEST_SIZE] for x in range(0, len(ids_to_fetch), self.BATCH_REQUEST_SIZE) ]

        for batch in batches:
            try:
                batch_ids = ','.join(batch)
                
                resp = self.youtube_svc.videos().list(
                    id=batch_ids, 
                    part='snippet').execute()
                
                self.fetch_contents(resp)
                
            except:            
                # If one batch fails, no need to fail everything else.
                print('\n\n\nERROR: Youtube handler fetch partial failure. Reason:%s\n\n\n' % (traceback.print_exc()))

        # Store all entries in one file and contents for each entry in their own files.
        store_path = history_store.prepare_to_store(self.name)
        entries = list(self.entries_to_fetch.values())
        #history_store.store_entries_metadata(self.name, entries, store_path=store_path)
        history_store.store_content(self.name, entries, store_path=store_path)
                
                
    def fetch_contents(self, resp):
        
        items = resp.get('items', None)
        if not items:
            return None
        
        # If any video ID is no longer available, it's not included in the response.
        # So it's possible some of the entries won't have contents.
        for v in items:
            video_id = v['id']
            v = v['snippet']
            title = v.get('title','')
            desc = v.get('description','')
            tags = v.get('tags', None)
            tags = ' '.join(tags) if tags else ''
                
            # Since LDA is a bag of words model, things like newlines and order of terms don't matter.
            # Just combine all the attributes into a single string.
            contents = ' '.join([title, desc, tags])
            
            self.entries_to_fetch[video_id]['contents'] = contents
        
      

# Module initialization
history_handlers.history_handlers.register(YoutubeHistoryHandler())

