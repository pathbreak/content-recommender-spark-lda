from __future__ import print_function
import os.path
import yaml
from pprint import pprint
import sys
import argparse

from history import HistoryProcessor
from history_store import HistoryStore

from targets import TargetsProcessor
from target_store import TargetStore

def recommend(args, app_conf):
    # Run LDA on spark and get topmost recommendation per topic.
    pass
    
def upload(args, app_conf):
    history_store = HistoryStore(app_conf)
    history = HistoryProcessor(app_conf)
    history.process_history(args.history_filepath, history_store)
    
def fetch(args, app_conf):
    target_store = TargetStore(app_conf)    
    targets_proc = TargetsProcessor(app_conf)
    targets_proc.fetch(target_store)
    
    
def read_conf():
    conf_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf')
    with open(os.path.join(conf_dir, 'conf.yml'), 'r') as conf_file:
        app_conf = yaml.load(conf_file)
    
    # Adjust relative paths.
    if app_conf['HISTORY_DIR'].startswith('.'):
        app_conf['HISTORY_DIR'] = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), app_conf['HISTORY_DIR']))
        
    if app_conf['TARGET_DIR'].startswith('.'):
        app_conf['TARGET_DIR'] = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), app_conf['TARGET_DIR']))
        
    app_conf['CONF_DIR'] = conf_dir
    
    return app_conf

def configure_arguments_parser():
    parser = argparse.ArgumentParser()
    
    actions = parser.add_subparsers(dest='cmd', title='commands')

    recommend_parser = actions.add_parser('recommend', help='Show recommendations from latest fetched contents')

    upload_parser = actions.add_parser('upload', help='Upload a browsing history JSON file')
    upload_parser.add_argument(dest='history_filepath', metavar='JSON-FILEPATH', 
        help='File path of browsing history JSON file.')

    fetch_parser = actions.add_parser('fetch', help='Download content from all configured targets (mainly meant for cron job)')
    
    args = parser.parse_args()
    return args
    

if __name__ == '__main__':
    args = configure_arguments_parser()
    print(args)
    
    app_conf = read_conf()
    pprint(app_conf)
    
    command_handlers = {
        'recommend' : recommend,
        'upload' : upload,
        'fetch': fetch
    }
    command_handlers[args.cmd](args, app_conf)

    
