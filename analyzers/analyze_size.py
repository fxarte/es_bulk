KEY_NAME='size in bytes'
def analyze_file(dir_entry):
    return dir_entry.stat(follow_symlinks=False).st_size

def analyze_folder(dir_entry, children_data):
    total=0
    for k, v in children_data.items():
        total += v[KEY_NAME]
    return total