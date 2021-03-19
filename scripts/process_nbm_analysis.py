from os.path import split, join
from util import process_directory, process_directory_parallel, nbm_analysis_fields, GridArrayNetCDF as GridArray
from datetime import timedelta, datetime

data_dir = 'data/nbm_analysis'

def group(files):
    output = {}
    for x in files:
        key = (datetime.strptime(split(x)[1][10:21], '%Y%m%d_%H') + timedelta(hours=1)).strftime('%Y%m%d')
        if key not in output:
            output[key] = []
        output[key].append(x)
    return [sorted(output[k]) for k in sorted(output.keys())]

def on_each(filename, archive, compress, output_function=print):
    if archive is None:
        dir_part, file_part = split(filename)
        dir_part = split(dir_part)[0]
        file_part = file_part[10:21].replace('_','')
        archive = GridArray(join(dir_part, 'NBM_Analysis_{0}.nc'.format((datetime.strptime(file_part, '%Y%m%d%H') + timedelta(hours=1)).strftime('%Y%m%d'))), nbm_analysis_fields, mode='w', compress=compress, forecast_hours=range(24), analysis_time=lambda a: (a + timedelta(hours=1)).replace(hour=0, minute=0, second=0, microsecond=0) )
    archive.append(filename, force_dtype='float32', output_function=output_function)
    return archive

def on_final(archive, output_function=print):
    archive.close(output_function=output_function)

def process(compress):
    process_directory(data_dir, lambda a,b : on_each(a, b, compress), on_final, filter_func=group)

def process_parallel(compress, processes):
    process_directory_parallel(data_dir, processes, on_each, on_final, on_each_args=[compress], filter_func=group)


if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv

    parser = ArgumentParser('process_nbm_analysis.py', description="Ingest GRIB2 NBM files into NetCDF4 files.")
    parser.add_argument('-u', '--uncompressed', dest='compress', action='store_false', help='Create uncompress NetCDF files')
    parser.add_argument('-p', '--processes', metavar='#', dest='processes', type=int, default=1, help='Number of sub-processes to use(default=1)')
    opts = parser.parse_args(argv[1:])

    # if opts.processes == 1:
    #     process(opts.compress)
    # else:
    process_parallel(opts.compress, opts.processes)
