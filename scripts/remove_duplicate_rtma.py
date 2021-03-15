from hashlib import sha256
from os.path import join
from os import listdir
from pygrib import open as open_grib
from util import rtma_fields, matches_fields, get_variable_info

def get_hash(data):
    hash = sha256()
    hash.update(data)
    return hash.hexdigest()

def process(path):
    duplicate_path = join(path, 'duplicate')
    incoming_path = join(path, 'incoming')
    for duplicate_file in listdir(duplicate_path):
        print('Processing {0:s} to {1:s}'.format(join(duplicate_path, duplicate_file), join(incoming_path, duplicate_file)))
        grb = open_grib(join(duplicate_path, duplicate_file))
        done = []
        found = []
        with open(join(path, 'incoming', duplicate_file), 'wb') as out_file:
            for i in range(grb.messages):
                msg = grb.message(i+1)
                hash = get_hash(msg.tostring())
                if hash not in done:
                    if matches_fields(msg, rtma_fields):
                        info = get_variable_info(msg, rtma_fields)
                        if info['variable_name'] not in found:
                            out_file.write(msg.tostring())
                            found.append(info['variable_name'])
                    done.append(hash)
        grb.close()

if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv

    parser = ArgumentParser('remove_duplicate.py', description="Remove duplicate messages from GRIB2 files")
    parser.add_argument('dataset', type=str, help='Directory with files to remove duplicates from')

    opts = parser.parse_args(argv[1:])

    process(opts.dataset)