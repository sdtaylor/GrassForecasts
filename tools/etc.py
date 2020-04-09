import glob
import os

def cmip_file_query(folder):
    file_list = glob.glob(folder+'/*.nc4')
    all_file_info = []
    for f in file_list:
        filename = os.path.basename(f)
        parts = filename.split('_')
        file_info = {'model'   : parts[4].lower(),
                     'scenario': parts[5],
                     'variable': parts[2],
                     'run'     : parts[6],
                     'decade'  : parts[7][0:4],
                     'full_path': f,
                     'filename':filename}
        
        all_file_info.append(file_info)

    return all_file_info



