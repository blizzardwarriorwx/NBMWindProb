from shutil import move
from os import listdir
from os.path import join

def reprocess(path):
    processed_path = join(path, 'processed')
    incoming_path = join(path, 'incoming')
    for filename in [x for x in listdir(processed_path) if x[0] != '.']:
        move(join(processed_path, filename), join(incoming_path, filename))

if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv
    
    parser = ArgumentParser('prepare_reprocess.py', description="Prepare files to be reprocessed by moving from processed to incoming.")
    parser.add_argument('dataset', type=str, help='Dataset to be repocessed')
    opts = parser.parse_args(argv[1:])

    reprocess(opts.dataset)

