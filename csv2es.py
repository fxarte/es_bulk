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

    

def get_csv_header(config, refresh=False):
    ''' need to get the header info in the order they appear in the file, 
    python dict doesnt garantees the order
    '''
    global cached_values
    if 'csv_header' not in cached_values or refresh:
        header = config.get('csv', 'header')
        if header:
            try:
                header = int(header)
                source_path = config.get('main', 'source_path')
                field_separator = config.get('csv', delimiter)
                with open(source_path, encoding='utf-8') as sourcefile:
                    for i, line in enumerate(sourcefile):
                        if i==header:
                            header = line.split(field_separator)
            except:
                header = header.split(",")
        cached_values['csv_header'] = header
    return cached_values['csv_header']
    
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
    header = get_csv_header(config)
    settings = {'mappings': {_type:{'properties':{k:{'type':'text'} for k in header}}}}
    es.indices.create(index=_index, body=settings)
    return es
    
def getcontent_csv(config):
    source_path = config.get('main', 'source_path')
    _index = config.get('main', 'index')
    _type = config.get('main', 'type')
    #TODO TEst for comma
    delimiter = str.encode(config.get('csv', 'delimiter'), 'utf-8').decode('unicode_escape')
    # source_path = os.path.abspath(os.path.join(os.path.dirname(__file__), source_path))

    header = get_csv_header(config)
    with open(source_path, encoding='utf-8') as sourcefile:
        datareader = csv.reader(sourcefile, delimiter=delimiter)

        for row in datareader:
            source_dict = {header[ind]:row[ind] for ind, x in enumerate(row)}
            yield {
                '_op_type': 'index',
                '_index': _index,
                '_type': _type,
                '_source': source_dict
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
    csv2es(parallel=True)

