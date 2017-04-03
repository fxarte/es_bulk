#!/usr/bin/python3
import ujson
from pathlib import Path
from urllib.parse import urlparse
import configparser
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk, parallel_bulk


def validate_item(item):
    return item

def recreate_index(config):
    host = config.get('main', 'host')
    port = config.get('main', 'port')
    
    _index = config.get('main', 'index')

    index_settings_file = config.get('main', 'index_settings')
    
    es = Elasticsearch([{'host': host, 'port': port}])
    # delete index if exists
    if es.indices.exists(_index):
        es.indices.delete(index=_index)
    # index settings
    settings={}
    #index_settings_file=None
    #print(index_settings_file)
    if index_settings_file:
        with open(index_settings_file) as json_file:
            settings = ujson.load(json_file)
    es.indices.create(index=_index, body=settings)
    return es
    

def push2es_stream(config):
    es = recreate_index(config)
    json_path = config.get('main', 'json_path')
    _index = config.get('main', 'index')
    _type = config.get('main', 'type')
    data=[]
    with open(json_path) as json_file:
        data = ujson.load(json_file)
    #Stream Bulk
    bulk_data = [{'_op_type': 'index','_index': _index,'_type': _type,'_source': validate_item(i)} for i in data]
    bulk(client=es, actions=bulk_data)
    
def push2es_parallel(config):
    es = recreate_index(config)
    json_path = config.get('main', 'json_path')
    _index = config.get('main', 'index')
    _type = config.get('main', 'type')
    #assumes data is a list of dictionaries
    def genereate_actions(data):
        for item in data:
            source_dict=item
            yield {
                '_op_type': 'index',
                '_index': _index,
                '_type': _type,
                '_source': source_dict
            }
    
    data=[]
    with open(json_path) as json_file:
        data = ujson.load(json_file)

    #paralell bulk
    for success, info in parallel_bulk(es, genereate_actions(data), thread_count=4):
        if not success: print('Doc failed', info)
    
    

def json2es(parallel=True):
    config = configparser.SafeConfigParser()
    config.read('config.ini')
    if parallel:
        push2es_parallel(config)
    else:
        push2es_stream(config)
        
    

if __name__ == '__main__':
    json2es(parallel=False):
    
    