#!/usr/bin/env python3
from __future__ import print_function
import os.path
import yaml
from pprint import pprint
import sys
import argparse
import subprocess

from history import HistoryProcessor
from history_store import HistoryStore

from targets import TargetsProcessor
from target_store import TargetStore

def recommend(args, app_conf):
    proc_path = os.path.join(
        args.spark_dir if args.spark_dir is not None else '/root/spark/stockspark/spark-2.1.1-bin-hadoop2.7/',
        'bin/spark-submit')
        
    proc_args = [
        proc_path,
        args.spark_job_jarpath if args.spark_job_jarpath is not None else '/root/spark/lda-prototype.jar',
        args.history_dir,
        args.target_dir,
        args.num_topics,
        args.num_iterations,
        'em',
        'custom_stopwords.txt'
    ]
    
    p = subprocess.Popen(proc_args, stdout=subprocess.PIPE)
 
    stdoutdata, stderrdata = p.communicate()
    print(stdoutdata.decode('utf-8'))
    
    
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
    recommend_parser.add_argument(dest='history_dir', metavar='HISTORY-DIRECTORY', 
        help='Directory where history contents have been stored by upload.')
    recommend_parser.add_argument(dest='target_dir', metavar='TARGET-DIRECTORY', 
        help='Directory where target contents have been stored by fetch.')
    recommend_parser.add_argument(dest='num_topics', metavar='NUMBER-OF-TOPICS', 
        help='Number of topics to discover. This depends on your interest and perceived quality of recommendations')
    recommend_parser.add_argument(dest='num_iterations', metavar='NUMBER-OF-ITERATIONS', 
        help='Number of iterations for LDA to execute.')
    recommend_parser.add_argument('--spark-dir', dest='spark_dir', metavar='SPARK-INSTALLATION-DIRECTORY', required=False,
        help='(Optional) Path of a Spark installation. Default: /root/spark/stockspark/spark-2.1.1-bin-hadoop2.7')
    recommend_parser.add_argument('--spark-jar', dest='spark_job_jarpath', metavar='SPARK-JOB-JAR-PATH', required=False,
        help='(Optional) Path of the Spark Job JAR. Default: /root/spark/lda-prototype.jar')
        
    upload_parser = actions.add_parser('upload', help='Upload a browsing history JSON file')
    upload_parser.add_argument(dest='history_filepath', metavar='JSON-FILEPATH', 
        help='File path of browsing history JSON file.')

    fetch_parser = actions.add_parser('fetch', help='Download content from all configured targets (mainly meant for cron job)')
    
    args = parser.parse_args()
    return args, parser
    

if __name__ == '__main__':
    args, parser = configure_arguments_parser()
    
    command_handlers = {
        'recommend' : recommend,
        'upload' : upload,
        'fetch': fetch
    }
    handler = command_handlers.get(args.cmd, None)
    if handler is None:
        parser.print_help()
        sys.exit(1)
        
    app_conf = read_conf()
    
    handler(args, app_conf)

    
