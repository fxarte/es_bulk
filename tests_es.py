#!/usr/bin/python3
''' Tests '''
import timeit
import pprint

def test_config_load():
    import config
    c = config.get()
    print(type(c))
    pprint.pprint(c)
    assert c.get('main', 'host') == 'localhost'
    assert c.get('main', 'port') == '9200'


if __name__ == '__main__':
    #print(timeit.timeit("test_config_load()", setup="from __main__ import test_config_load"), number=1)
    test_config_load()