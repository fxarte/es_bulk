#!/usr/bin/python3
#Configs
import configparser

def get():
    print("Loading configs")
    config = configparser.SafeConfigParser()
    config.read('config.ini')
    return config
