from util import matches_fields, nbm_fields
from io import BytesIO
from pygrib import fromstring
from datetime import datetime, timedelta
from os.path import join, exists
from urllib.request import urlopen
from time import sleep

def download1(cycle):
    if cycle < datetime(2020,9,29,12):
        url = cycle.strftime('https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.%Y%m%d/%H/grib2/blend.t%Hz.master.f{0:03d}.co.grib2')
    elif cycle >= datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1):
        url = cycle.strftime('https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod/blend.%Y%m%d/%H/core/blend.t%Hz.core.f{0:03d}.co.grib2')
    else:
        url = cycle.strftime('https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.%Y%m%d/%H/core/blend.t%Hz.core.f{0:03d}.co.grib2')
    filename = cycle.strftime('nbm_conus_%Y%m%d_%HZ_f{0:03d}.grib2')
    for hour in list(range(1,37,1)) + list(range(39, 169, 3)):
        hour_url = url.format(hour)
        hour_filename = join('data', 'nbm', 'incoming', filename.format(hour))
        if not exists(hour_filename):
            with BytesIO() as content:
                print(hour_url)
                nbm = urlopen(hour_url)
                content.write(nbm.read())
                nbm.close()
                end = content.tell()
                loc = 0
                with open(hour_filename, 'wb') as out_file:
                    print('    -> {0:s}'.format(hour_filename))
                    while loc < end:
                        content.seek(loc)
                        msg = fromstring(content.read())
                        if matches_fields(msg, nbm_fields):
                            out_file.write(msg.tostring())
                        loc += len(msg.tostring())
            sleep(5)

def download(cycle, hour, output_function=print):
    if cycle < datetime(2020,9,29,12):
        url = cycle.strftime('https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.%Y%m%d/%H/grib2/blend.t%Hz.master.f{0:03d}.co.grib2').format(hour)
    elif cycle >= datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1):
        url = cycle.strftime('https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod/blend.%Y%m%d/%H/core/blend.t%Hz.core.f{0:03d}.co.grib2').format(hour)
    else:
        url = cycle.strftime('https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.%Y%m%d/%H/core/blend.t%Hz.core.f{0:03d}.co.grib2').format(hour)
    filename = join('data', 'nbm', 'incoming', cycle.strftime('nbm_conus_%Y%m%d_%HZ_f{0:03d}.grib2').format(hour))
    with BytesIO() as content:
        output_function('{0:>11s}: {1:s}'.format('Downloading', url))
        end = None
        try:
            nbm = urlopen(url)
            content.write(nbm.read())
            nbm.close()
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
    sleep(5)


if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv
    from util import ProcessPool
    
    parser = ArgumentParser('download_nbm.py', description="Download GRIB2 NBM files")
    parser.add_argument('date', metavar="date", type=str, help="Date to download in YYYYMMDD")
    parser.add_argument('-9', dest='hour', action='store_const', const=9, help="Donwload 09Z cycle")
    parser.add_argument('-21', dest='hour', action='store_const', const=21, help="Donwload 21Z cycle")
    parser.add_argument('-n', '--ncycles', metavar='N', dest='number_cycles', type=int, default=1, help='Number of cyles to download')
    parser.add_argument('-p', '--processes', metavar='#', dest='processes', type=int, default=1, help='Number of sub-processes to use(default=1)')
    opts = parser.parse_args(argv[1:])

    if opts.hour is None:
        print('Must chose a cycle, -9 or -21\n')
        parser.parse_args(['-h'])
    
    initial_cycle = datetime.strptime(opts.date, '%Y%m%d') + timedelta(hours=opts.hour)
    # for i in range(opts.number_cycles):
    #     cycle = initial_cycle + i * timedelta(hours=12)
    #     if cycle < datetime.utcnow():
    #         download(cycle)
    
    todo_list = []

    for i in range(opts.number_cycles):
        cycle = initial_cycle + i * timedelta(hours=12)
        if cycle < datetime.utcnow():
            for hour in list(range(1,37,1)) + list(range(39, 169, 3)):
                if not exists(join('data', 'nbm', 'incoming', cycle.strftime('nbm_conus_%Y%m%d_%HZ_f{0:03d}.grib2').format(hour))):
                    todo_list.append((cycle, hour))
    
    if opts.processes == 1:
        for x in todo_list:
            download(*x)
    else:
        p = ProcessPool(opts.processes)
        p.map(download, todo_list)
