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


class Simple_Elastic_Search_Class():
    def push(**kwargs):
        index = settings.get('main','index')
        type = settings.get('main','type')
        if 'data' in kwargs:
            data = kwargs['data']
            return es.index(index=index, doc_type=type, id=data['id'], body=data)
        else:
            return None
        
        
        
def get_target():
    return Simple_Elastic_Search_Class
        
        
class Analyze_size():
    KEY_NAME='size in bytes'
    def get_key_name():
        return Analyze_size.KEY_NAME

    def analyze_file(dir_entry):
        return 0
        
    def analyze_folder(dir_entry, children_data):
        total = 1
        for k, v in children_data.items():
            total += v[Analyze_size.KEY_NAME]
        return total


def get_analyzers():
    ''' returns analyzers'''
    return [Analyze_size]

        
def process_file(dir_entry):
    file_data={"id":dir_entry.path}
    for a in get_analyzers():
        file_data[a.get_key_name()] = a.analyze_file(dir_entry)
    return file_data

def post_visit_process_folder(dir_path, children_data=None):
    folder_data = {"id":dir_path}
    for a in get_analyzers():
        folder_data[a.get_key_name()] = a.analyze_folder(dir_path, children_data)
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
    print("    done!")
    return folder_data


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
    #config_path = 'files_config.ini'
    settings = conf.get_config(config_path)
    path = settings.get('main', 'source_path')
    es = eso.get_index(config_path)
    #eso.push2es(es, settings, crawl_filesystem(path, settings))
    #success, _ = bulk(es, crawl_filesystem(path, settings))
    visit_tree(path)
