#!/usr/bin/python3
''' Python script to find duplicate files and folders'''
from functools import lru_cache
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk, parallel_bulk
import sys
import os
import argparse

import config as conf
import es_operations as eso
import hashlib


ANALYZERS=None

class Simple_Elastic_Search_Class():
    def push(**kwargs):
        index = settings.get('main','index')
        type = settings.get('main','type')
        if 'data' in kwargs:
            data = kwargs['data']
            # in this case we do not want to keep id as a field:
            id = data['id']
            del data['id']
            return es.index(index=index, doc_type=type, id=id, body=data)
        else:
            return None
        
        
        
def get_target():
    return Simple_Elastic_Search_Class
        
        
class HashKeySHA1():
    ''' File hash calculation '''
    
    KEY_NAME='a hash value'

    def analyze_file(dir_entry):
        '''
        Use this shell as reference:
        (stat --printf="blob %s\0" "$1"; cat "$1") | sha1sum -b | cut -d" " -f1
        '''
        s = hashlib.sha1()
        filesize_bytes = dir_entry.stat(follow_symlinks=False).st_size
        s.update(("blob %u\0" % filesize_bytes).encode('utf-8'))
        with open(dir_entry.path, 'rb') as f:
            s.update(f.read())
        return s.hexdigest()

    def analyze_folder(dir_entry, children_data):
        total = ""
        for k, v in children_data.items():
            total +=" " + v[HashKeySHA1.KEY_NAME]
        return total
    
    
    
def process_file(dir_entry):
    hash = HashKeySHA1.analyze_file(dir_entry)
    file_data={"id":"{}#{}".format(dir_entry.path, hash)}
    file_data[HashKeySHA1.KEY_NAME] = hash
    for a in ANALYZERS:
        file_data[a.KEY_NAME] = a.analyze_file(dir_entry)
    return file_data

def post_visit_process_folder(dir_path, children_data=None):
    folder_data = {"id":dir_path}
    for a in ANALYZERS:
        folder_data[a.KEY_NAME] = a.analyze_folder(dir_path, children_data)
    return folder_data

def visit_tree(path):
    """Return total size of files in given path and subdirs."""
    print("Start processing folder: '{}'".format(path))
    children_data = {}
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            #add here a pre visit call, if needed
            children_data[entry.path] = visit_tree(entry.path)
        else:
            #tree_data += entry.stat(follow_symlinks=False).st_size
            file_data = process_file(entry)
            # send data to target per file
            get_target().push(entry=entry, data=file_data)
            children_data[entry.path] = file_data
    
    #post visit call
    folder_data = post_visit_process_folder(path, children_data)
    # send folder data to target after pst visit processing
    get_target().push(data=folder_data)
    # We return folder_data for parent calculations
    return folder_data


def load_analyzers():
    global ANALYZERS
    if not ANALYZERS:
        ANALYZERS=[]
        analyzers_modules = __import__('analyzers')
        for a in analyzers_modules.__all__:
            ANALYZERS.append(getattr(analyzers_modules, a))





if __name__ == '__main__':
    print(__doc__)
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--help', action="help")
    parser.add_argument(
        '-p', '--path',
        dest='path',
        metavar='',
        required=True,
        help='Required: Path to the configuration file'
        #default=str(Path(__file__).resolve().parent / 'config')
    )
    args = parser.parse_args()
    config_path = args.path

    load_analyzers()


    #config_path = 'files_config.ini'
    settings = conf.get_config(config_path)
    path = settings.get('main', 'source_path')
    es = eso.get_index(config_path)
    #eso.push2es(es, settings, crawl_filesystem(path, settings))
    #success, _ = bulk(es, crawl_filesystem(path, settings))
    visit_tree(path)
