import es_operations as eso

KEY_NAME='elastic search simple'
SETTINGS=None
def push(**kwargs):
    config_path = SETTINGS.get('main', 'config_path')
    es = eso.get_index(config_path)

    index = SETTINGS.get('main','index')
    type = SETTINGS.get('main','type')
    if 'data' in kwargs:
        data = kwargs['data']
        # in this case we do not want to keep id as a field:
        id = data['id']
        del data['id']
        return es.index(index=index, doc_type=type, id=id, body=data)
    else:
        return -1