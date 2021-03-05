from os.path import split, join
from util import process_directory
from util import GridArchive, nbm_fields

data_dir = 'data/nbm'

def group(files):
    output = {}
    for x in files:
        key = split(x)[1][10:21]
        if key not in output:
            output[key] = []
        output[key].append(x)
    return [sorted(output[k]) for k in sorted(output.keys())]

def on_each(filename, archive, compress=False):
    if archive is None:
        dir_part, file_part = split(filename)
        dir_part = split(dir_part)[0]
        file_part = file_part[10:21].replace('_','')
        archive = GridArchive(join(dir_part, 'NBM_{0}.nc'.format(file_part)), nbm_fields, mode='w', compress=compress)
    archive.append(filename)
    return archive

def process(compress):
    process_directory(data_dir, lambda a,b : on_each(a, b, compress), lambda a:a.close(), filter_func=group)


if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv

    parser = ArgumentParser('process_nbm.py', description="Ingest GRIB2 NBM files into NetCDF4 files.")
    parser.add_argument('-u', '--uncompressed', dest='compress', action='store_false', help='Create uncompress NetCDF files')
    opts = parser.parse_args(argv[1:])

    process(opts.compress)
