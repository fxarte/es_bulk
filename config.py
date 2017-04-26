#Configs
import configparser

def load(config_path=None):
    config = configparser.SafeConfigParser()
    if not config_path:
        config_path = 'config.ini'
    
    config.read(config_path)

    #config.add_section('tempSection')
    config.set('main', 'config_path', config_path)
    return config
