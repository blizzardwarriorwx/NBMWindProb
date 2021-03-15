from urllib.request import urlopen
from os.path import join
from datetime import datetime, timedelta
from io import BytesIO
from pygrib import fromstring
from urllib.error import HTTPError
from util import matches_fields, rtma_fields, file_exists
from time import sleep

def download(date, output_function=print):
    file_name = join('data', 'rtma', 'incoming', date.strftime('RTMA_%Y%m%d%H.grib2'))
    with BytesIO() as content:
        loc = 0
        if date >= datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=3):
            end = download_ncep(date, content, output_function=output_function)
        else:
            end = download_ncei(date, content, output_function=output_function)
        if loc < end:
            with open(file_name, 'wb') as out_file:
                output_function('{0:>11s}: {1:s}'.format('Writing', file_name))
                while loc < end:
                    content.seek(loc)
                    msg = fromstring(content.read())
                    if matches_fields(msg, rtma_fields):
                        out_file.write(msg.tostring())
                    loc += len(msg.tostring())
        
def download_ncei(date, out_file, output_function=print):
    file_fields = ['LEI', 'LHI', 'LPI', 'LTI', 'LRI', 'LNI']
    url = date.strftime('https://www.ncei.noaa.gov/data/national-digital-guidance-database/access/%Y%m/%Y%m%d/{0:3s}A98_KWBR_%Y%m%d%H00')
    for field in file_fields:
        output_function('{0:>11s}: {1:s}'.format('Downloading', url.format(field)))
        try:
            ncdc = urlopen(url.format(field), timeout=10)
            out_file.write(ncdc.read())
            ncdc.close()
            sleep(3)
        except:
            output_function('Not Found')
    size = out_file.tell()
    out_file.seek(0)
    return size

def download_ncep(date, out_file, output_function=print):
    for url in [date.strftime('https://nomads.ncep.noaa.gov/pub/data/nccf/com/rtma/prod/rtma2p5.%Y%m%d/rtma2p5.%Y%m%d%H.pcp.184.grb2'),
                date.strftime('https://nomads.ncep.noaa.gov/pub/data/nccf/com/rtma/prod/rtma2p5.%Y%m%d/rtma2p5.t%Hz.2dvaranl_ndfd.grb2_wexp')]:
        output_function('{0:>11s}: {1:s}'.format('Downloading', url))
        try:
            ncep = urlopen(url, timeout=10)
            out_file.write(ncep.read())
            ncep.close()
            sleep(3)
        except:
            output_function('Not Found')
    size = out_file.tell()
    out_file.seek(0)
    return size

if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv
    from util import ProcessPool

    parser = ArgumentParser('download_rtma.py', description="Download RMTA data.")
    parser.add_argument('date', metavar="date", type=str, help="Date to download in YYYYMMDDHH")
    parser.add_argument('-n', '--ncycles', metavar='N', dest='number_cycles', type=int, default=1, help='Number of hours to download')
    parser.add_argument('-p', '--processes', metavar='#', dest='processes', type=int, default=1, help='Number of sub-processes to use(default=1)')
    opts = parser.parse_args(argv[1:])

    todo_list = []

    initial_cycle = datetime.strptime(opts.date, '%Y%m%d%H')
    for i in range(opts.number_cycles):
        cycle = initial_cycle + timedelta(hours=i)
        if cycle < datetime.utcnow():
            if not file_exists(join('data','rtma'), cycle.strftime('RTMA_%Y%m%d%H.grib2')):
                todo_list.append((cycle,))
    
    if opts.processes == 1:
        for x in todo_list:
            download(*x)
    else:
        p = ProcessPool(opts.processes)
        p.map(download, todo_list)
