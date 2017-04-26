#!/usr/bin/python3
''' Python script to analyze a file system tree'''
from functools import lru_cache
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk, parallel_bulk
import sys
import os
import argparse
import hashlib

import config as conf
#import es_operations as eso


ANALYZERS=None
TARGETS=None

        
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
    #print("Start processing folder: '{}'".format(path))
    children_data = {}
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            #add here a pre visit call, if needed
            children_data[entry.path] = visit_tree(entry.path)
        else:

            #tree_data += entry.stat(follow_symlinks=False).st_size
            file_data = process_file(entry)

            # send data to target per file
            #print("Pushing file data to targets")
            res = [t.push(entry=entry, data=file_data) for t in TARGETS]

            children_data[entry.path] = file_data
    
    #post visit call
    folder_data = post_visit_process_folder(path, children_data)
    # send folder data to target after pst visit processing
    #print("Pushing folder data to targets")
    res = [t.push(entry=entry, data=folder_data) for t in TARGETS]
    # We return folder_data for parent calculations
    return folder_data


def load_analyzers():
    global ANALYZERS
    if not ANALYZERS:
        ANALYZERS=[]
        analyzers_modules = __import__('analyzers')
        for a in analyzers_modules.__all__:
            ANALYZERS.append(getattr(analyzers_modules, a))


def load_targets():
    global TARGETS
    if not TARGETS:
        TARGETS=[]
        targets_modules = __import__('targets')
        for t in targets_modules.__all__:
            m = getattr(targets_modules, t)
            m.SETTINGS=settings
            TARGETS.append(m)





if __name__ == '__main__':
    print(__doc__)
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--help', action="help")
    parser.add_argument(
        '-p', '--path',
        dest='path',
        metavar='',
        help='Path to the configuration file'
        #default=str(Path(__file__).resolve().parent / 'config')
    )
    args = parser.parse_args()
    config_path = args.path
    settings = conf.load(config_path)

    load_analyzers()
    load_targets()


    #config_path = 'files_config.ini'
    path = settings.get('main', 'source_path')
    #es = eso.get_index(config_path)
    #eso.push2es(es, settings, crawl_filesystem(path, settings))
    #success, _ = bulk(es, crawl_filesystem(path, settings))
    visit_tree(path)
