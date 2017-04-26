''' From: http://stackoverflow.com/a/1057534 '''
from os.path import dirname, basename, isfile
from os import listdir, scandir
import glob
# TODO: ADD some naming convention?
#__all__=['analyze_size']
#modules = listdir(dirname(__file__))
def is_python_file(entry):
    return not entry.is_dir() and entry.path.endswith('py') and not entry.path.endswith('__')
modules = [e.path for e in scandir(dirname(__file__)) if is_python_file(e)]
#print(basename(modules[1]))
__all__ = [ basename(f)[:-3] for f in modules if isfile(f) and not basename(f)[:-3].startswith('__')]
from . import *