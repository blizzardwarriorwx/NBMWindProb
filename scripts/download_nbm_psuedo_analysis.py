from util import matches_fields, nbm_fields
from io import BytesIO
from pygrib import fromstring
from datetime import datetime, timedelta
from os.path import join, exists
from urllib.request import urlopen
from time import sleep
from random import random

def download(cycle, hour, output_function=print):
    if cycle < datetime(2020,9,29,12):
        url = cycle.strftime('https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.%Y%m%d/%H/grib2/blend.t%Hz.master.f{0:03d}.co.grib2').format(hour)
    elif cycle >= datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1):
        url = cycle.strftime('https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod/blend.%Y%m%d/%H/core/blend.t%Hz.core.f{0:03d}.co.grib2').format(hour)
    else:
        url = cycle.strftime('https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.%Y%m%d/%H/core/blend.t%Hz.core.f{0:03d}.co.grib2').format(hour)
    filename = join('data', 'nbm_analysis', 'incoming', cycle.strftime('nbm_conus_%Y%m%d_%HZ_f{0:03d}.grib2').format(hour))
    with BytesIO() as content:
        output_function('{0:>11s}: {1:s}'.format('Downloading', url))
        end = None
        try:
            nbm = urlopen(url, timeout=10)
            content.write(nbm.read())
            nbm.close()
            sleep(3 + 2 * random())
            end = content.tell()
            loc = 0
        except:
            output_function('Not Found')
        if end is not None:
            with open(filename, 'wb') as out_file:
                output_function('{0:>11s}: {1:s}'.format('Writing', filename))
                while loc < end:
                    content.seek(loc)
                    msg = fromstring(content.read())
                    if matches_fields(msg, nbm_fields):
                        out_file.write(msg.tostring())
                    loc += len(msg.tostring())


if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv
    from util import ProcessPool
    
    parser = ArgumentParser('download_nbm_psuedo_analysis.py', description="Download GRIB2 NBM Analysis files")
    parser.add_argument('date', metavar="date", type=str, help="Date to download in YYYYMMDD")
    parser.add_argument('-n', '--ncycles', metavar='N', dest='number_cycles', type=int, default=1, help='Number of cyles to download')
    parser.add_argument('-p', '--processes', metavar='#', dest='processes', type=int, default=1, help='Number of sub-processes to use(default=1)')
    opts = parser.parse_args(argv[1:])
    
    initial_cycle = datetime.strptime(opts.date, '%Y%m%d')
    
    todo_list = []

    for i in range(opts.number_cycles):
        cycle = initial_cycle + timedelta(hours=i)
        if cycle < datetime.utcnow():
            if not exists(join('data', 'nbm_analysis', 'incoming', cycle.strftime('nbm_conus_%Y%m%d_%HZ_f{0:03d}.grib2').format(1))):
                todo_list.append((cycle, 1))
    
    if opts.processes == 1:
        for x in todo_list:
            download(*x)
    else:
        p = ProcessPool(opts.processes)
        p.map(download, todo_list)
