#!/usr/bin/python3
'''Provides quick easy Elastic Search bulk operations provided a list of dicts
@author: Felix A Silberstein

'''

import configparser
import pprint

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk, parallel_bulk

def interesting_lines(f):
    ''' taken from https://youtu.be/EnSu9hHGq5o?t=18m41s
        f can be any value that can produce strings: a text file, a list of strings
    '''
    for line in f:
        line = line.strip()
        if line.startswith('#'):
            continue
        if not line:
            continue
        yield line

#@lru_cache(maxsize=8)
def get_index(config, refresh=True):
    '''Returns an ES index, either a cached or fresh one '''
    host = config.get('main', 'host')
    port = config.get('main', 'port')
    
    index = config.get('main', 'index')
    index_settings_file = config.get('main', 'index_settings')
    
    es = Elasticsearch([{'host': host, 'port': port}])
    # delete index if exists
    if es.indices.exists(index):
        es.indices.delete(index=index)
    # index settings
    settings={}
    #index_settings_file=None
    #print('------', index_settings_file)
    if index_settings_file:
        try:
            with open(index_settings_file) as source_file:
                #print('File READ')
                settings = ujson.load(source_file)
        except:
            print("Error while reading file: '{}'".format(index_settings_file), sys.exc_info()[0])
            print("No index file found, using ES defaults")
    es.indices.create(index=index, body=settings)
    return es

def push2es(config,data, parallel=False):
    ''' Pushes a list of dicts to the associated Elastic Search instance '''
    es = get_index(config)
    source_path = config.get('main', 'source_path')
    _index = config.get('main', 'index')
    _type = config.get('main', 'type')
    parser = config.get('xml', 'parser')
    #assumes data is a list of dictionaries
    #Generator version
    def genereate_actions(data):
        for item in data:
            action = {
                '_op_type': 'index',
                '_index': _index,
                '_type': _type,
                '_source': {}
            }
            if '_id' in item:
                action['_id'] = item['_id']
                del item['_id']
            action['_source'] = item
            yield action
    
    def get_actions(data):
        return list(genereate_actions(data))
        
    if parallel:
        #paralell bulk
        for success, info in parallel_bulk(es, genereate_actions(data), thread_count=4):
            if not success: print('Doc failed', info)
    else:
        bulk(client=es, actions=get_actions(data))


if __name__ == '__main__':
    # performance benchmark: timeit function_call(args)
    # Config: http://stackoverflow.com/questions/7443366/argument-passing-strategy-environment-variables-vs-command-line
    print(__doc__)
    #xml2es(parallel=False)
    #xml2es()
