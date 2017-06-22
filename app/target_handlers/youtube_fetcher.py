from __future__ import print_function
from apiclient import discovery
import json
import datetime
import traceback
import os
from pprint import pprint

import target_handlers

class YoutubeFetcher(object):
    '''
    Uses Youtube v3 API's search endpoint to get all videos published in the last few hours.
    
    It uses the 'period' attribute from conf.yml and asks youtube to give details of only
    those videos uploaded in the last 'period' number of hours.
    
    If there's a 'query' attribute in conf.yml, it's used to ask youtube for only videos
    matching that query. Since the whole idea of topic modelling is to go beyond search queries
    and look for latent relationships between web resources, it's recommended to not use this
    unless it's a very very general query term.
    '''
    
    DEFAULT_PERIOD = 6  # Hours, in case there's no 'period' in conf.yml. 
    API_KEY_FILE = 'yt_api_key.txt'
    
    def __init__(self):
        self.type = 'youtube'
        print('youtube fetcher instance')
        
    
    def conf_init(self, app_conf, handler_conf):
        '''
        app_conf: the full application configuration, including all contents of conf.yml
        handler_conf: configuration attributes specific to this instance of the handler
        '''
        self.app_conf = app_conf
        self.handler_conf = handler_conf
        self.youtube_svc = self.get_youtube_service()
        
        self.name = handler_conf.get('name', None)
        if not self.name:
            raise RuntimeError('conf.yml error: one of the youtube TARGETS is missing name attribute')
            
        period = handler_conf.get('period', self.DEFAULT_PERIOD)
        self.period_delta = datetime.timedelta(hours=int(period))
        
        self.query = handler_conf.get('query', None)
        
        
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



    def fetch(self, target_store):
        # Youtube API's search.list expects publishedAfter timestamp 
        # in RFC 3339 format like '2017-06-12T00:00:00Z'
        published_after = datetime.datetime.utcnow() - self.period_delta
        published_after = published_after.isoformat('T') + 'Z'

        print("Fetching latest YouTube video details published since %s, matching query:%s"%(published_after, self.query))
        
        try:
            req = self.youtube_svc.search().list(
                part='snippet', type='video', 
                q=self.query, 
                publishedAfter=published_after)
            
            resp = req.execute()
        except:
            print('\n\n\nERROR: Youtube target handler search could not be initiated. Reason:%s\n\n\n' % (traceback.print_exc()))
            return
            
        # Store every fetched content along with its metadata in its own file.
        store_path = target_store.prepare_to_store(self.name)
        
        while resp is not None:
            # Every video resource in the response is converted to a text document using its
            # attributes.
            entries = self.create_entries(resp)
            if entries:
                pprint(entries)
                target_store.store_content(self.name, entries, store_path=store_path)
        
            # Next page of requests
            try:
                req = self.youtube_svc.search().list_next(req, resp)
                if req is None:
                    print('\n\n\nYoutube target handler: Search completed') 
                    break
                    
                resp = req.execute()
            except:
                print('\n\n\nERROR: Youtube target handler search partial failure. Reason:%s\n\n\n' % (traceback.print_exc()))
        
        
        
    def create_entries(self, resp):
        videos = resp.get('items', None)
        if not videos:
            return None
            
        entries = []
        for v in videos:
            video_id = v['id']['videoId']
            v = v['snippet']
            title = v.get('title','')
            desc = v.get('description','')
            tags = v.get('tags', None)
            tags = ' '.join(tags) if tags else ''
                
            # Since LDA is a bag of words model, things like newlines and order of terms don't matter.
            # Just combine all the attributes into a single string.
            contents = ' '.join([title, desc, tags])
            
            entry = {
                'id': self.name + '-' + video_id,
                'url' : 'https://www.youtube.com/watch?v=' + video_id,
                'title' : title,
                'details' : desc,
                'video_id' : video_id,
                'contents': contents
            }
            
            entries.append(entry)
        
        return entries
        

#-----------------------------------------------------------------------
# Module entry point
target_handlers.target_handlers.register(YoutubeFetcher())
