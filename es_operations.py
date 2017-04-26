#!/usr/bin/python3
'''Provides quick easy Elastic Search bulk operations provided a list of dicts
@author: Felix A Silberstein

'''

#python
from functools import lru_cache
import sys

#libraries
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk, parallel_bulk

#
import config as conf

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

@lru_cache(maxsize=16)
def get_index(config_path, refresh=False):
    '''Returns an ES index, if it does not exists, or if refresh is True, it will be created.
    Otherwise an instance poiunting to an existing one will be returned '''
    settings = conf.load(config_path)
    host = settings.get('main', 'host')
    port = settings.get('main', 'port')
    
    index = settings.get('main', 'index')
    index_settings_file = settings.get('main', 'index_settings')
    
    es = Elasticsearch([{'host': host, 'port': port}])
    # delete index if exists
    if es.indices.exists(index):
        es.indices.delete(index=index)
    # index settings
    settings={}
    if index_settings_file:
        try:
            with open(index_settings_file) as source_file:
                settings = ujson.load(source_file)
        except:
            print("Error while reading file: '{}'".format(index_settings_file), sys.exc_info()[0])
            print("No index setting file found, using ES defaults")
            pass
    es.indices.create(index=index, body=settings)
    return es

def push2es(es, settings, data, parallel=False):
    ''' Pushes a list of dicts to the associated Elastic Search instance '''
    #es = get_index(settings)
    source_path = settings.get('main', 'source_path')
    _index = settings.get('main', 'index')
    _type = settings.get('main', 'type')
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
        bulk(client=es, actions=genereate_actions(data))


if __name__ == '__main__':
    # performance benchmark: timeit function_call(args)
    # Config: http://stackoverflow.com/questions/7443366/argument-passing-strategy-environment-variables-vs-command-line
    print(__doc__)
    #xml2es(parallel=False)
    #xml2es()
