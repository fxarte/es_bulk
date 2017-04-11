#!/usr/bin/python3
import xml.etree.ElementTree as etree
import configparser
from importlib import import_module
import pprint

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk, parallel_bulk


def validate_item(item):
    return item

def parseXML(xml_file, parser=None, id_field=None):
    '''
    parser name of the file that contains a parser implementation
    '''
    if parser is not None:
        parser_module = import_module(parser)
        return parser_module.parse(xml_file, id_field=id_field)
    else:
        pass


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
        try:
            with open(index_settings_file) as source_file:
                settings = ujson.load(source_file)
        except:
            print("No index file found, using ES defaults")
    es.indices.create(index=_index, body=settings)
    return es


def push2es_stream(config):
    es = recreate_index(config)
    source_path = config.get('main', 'source_path')
    _index = config.get('main', 'index')
    _type = config.get('main', 'type')
    parser = config.get('xml', 'parser')
    id_field = config.get('main', 'id_field', fallback=None)

    data=[]
    #with open(source_path) as source_file:
    data = parseXML(source_path, parser=parser, id_field=id_field)
    #Stream Bulk
    bulk_data = [{'_op_type': 'index','_index': _index,'_type': _type,'_source': validate_item(i)} for i in data]
    bulk(client=es, actions=bulk_data)
    
def push2es_parallel(config):
    es = recreate_index(config)
    source_path = config.get('main', 'source_path')
    _index = config.get('main', 'index')
    _type = config.get('main', 'type')
    parser = config.get('xml', 'parser')
    id_field = config.get('main', 'id_field', fallback=None)
    #assumes data is a list of dictionaries
    def genereate_actions(data):
        for item in data:
            action = {
                '_op_type': 'index',
                '_index': _index,
                '_type': _type,
                '_source': item
            }
            if '_id' in item:
                action['_id'] = item['_id']
                del item['_id']
            yield action
    
    data=[]
    data = parseXML(source_path, parser=parser, id_field=id_field)

    #paralell bulk
    for success, info in parallel_bulk(es, genereate_actions(data), thread_count=4):
        if not success: print('Doc failed', info)


def xml2es(parallel=True):
    config = configparser.SafeConfigParser()
    config.read('config.ini')
    if parallel:
        push2es_parallel(config)
    else:
        push2es_stream(config)

if __name__ == '__main__':
    #xml2es(parallel=False)
    xml2es()
