from os.path import split, join, exists
from os import makedirs
from shutil import move

def process_verify_file(filename):
    with open(filename, 'r') as in_file:
        data = in_file.read().strip().split('\n')
    for row in data:
        file_path, status = row.split(': ')
        if status.find('Missing fields') > -1:
            dir_part, file_part = split(file_path)
            dir_part, _ = split(dir_part)
            if not exists(join(dir_part, 'missing')):
                makedirs(join(dir_part, 'missing'))
            move(file_path, join(dir_part, 'missing', file_part))
            print('Moving missing')
        elif status.find('Duplicate field') > -1:
            dir_part, file_part = split(file_path)
            dir_part, _ = split(dir_part)
            if not exists(join(dir_part, 'duplicate')):
                makedirs(join(dir_part, 'duplicate'))
            move(file_path, join(dir_part, 'duplicate', file_part))
            print('Moveing duplicate')
        else:
            dir_part, file_part = split(file_path)
            dir_part, _ = split(dir_part)
            if not exists(join(dir_part, 'processed')):
                makedirs(join(dir_part, 'processed'))
            move(file_path, join(dir_part, 'processed', file_part))
            print('Moving good file')

if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv

    parser = ArgumentParser('filter_verify.py', description="Filter output from verify scripts")
    parser.add_argument('file', type=str, help='Verfiy output file to filter')
    opts = parser.parse_args(argv[1:])
    
    process_verify_file(opts.file)