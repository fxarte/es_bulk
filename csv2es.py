#!/usr/bin/python3
import csv, os
from pathlib import Path
from urllib.parse import urlparse
import configparser
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk, parallel_bulk

cached_values={}

def validate_item(item):
    return item


def recreate_index(config):
    host = config.get('main', 'host')
    port = config.get('main', 'port')
    
    _index = config.get('main', 'index')
    _type = config.get('main', 'type')

    index_settings_file = config.get('main', 'index_settings')
    
    es = Elasticsearch([{'host': host, 'port': port}])
    # delete index if exists
    if es.indices.exists(_index):
        es.indices.delete(index=_index)
    # index settings
    settings={}
    es.indices.create(index=_index, body=settings)
    return es
    
def getcontent_csv(config):
    
    source_path = config.get('main', 'source_path')
    _index = config.get('main', 'index')
    _type = config.get('main', 'type')
    fieldnames = config.get('csv', 'header', fallback=None)
    #Parse field names if set
    fieldnames=fieldnames.split(',') if fieldnames is not None else None
    # Added support multi-character represented characters, such as tabs.
    delimiter = str.encode(config.get('csv', 'delimiter'), 'utf-8').decode('unicode_escape')
    
    with open(source_path, encoding='utf-8') as f:
        csvreader = csv.DictReader(f, fieldnames=fieldnames, delimiter=delimiter)
        for row in csvreader:
            yield {
                '_op_type': 'index',
                '_index': _index,
                '_type': _type,
                '_source': row
            }


def push2es_stream(config):
    es = recreate_index(config)
    print(bulk(es, getcontent_csv(config)))
    
    
def push2es_parallel(config):
    es = recreate_index(config)
    #paralell bulk
    for success, info in parallel_bulk(es, getcontent_csv(config), thread_count=4):
        if not success: print('Doc failed', info)
    
    
    
def csv2es(parallel=True):
    config = configparser.SafeConfigParser()
    config.read('config.ini')
    if parallel:
        push2es_parallel(config)
    else:
        push2es_stream(config)
        
    

if __name__ == '__main__':
    #csv2es(parallel=True)
    csv2es()

