"""
Process METARs from Iowa State - Iowa Enviromental Mesonet (https://mesonet.agron.iastate.edu/request/download.phtml)
"""
from re import search
from pandas import read_csv, to_datetime, concat, DataFrame
from os.path import join, exists
from os import makedirs
from pint import UnitRegistry
from xarray import open_dataset

units = UnitRegistry()

location_file = None

def read_gempak_locations(filename):
    fields = [0, 6, 15, 48, 51, 54, 60, 67, 73]

    with open(filename, 'r') as in_file:
        data = DataFrame([dict([(['Site', 'WBAN', 'StationName','State', 'Country', 'Latitude', 'Longitude', 'Elevation'][i], [str, lambda a: None if a == '' else int(a), str, str, str, lambda a:float(a)/100.0, lambda a: float(a)/100, float][i](x[y[0]:y[1]].strip())) for i, y in enumerate(zip(fields[:-1], fields[1:]))]) for x in in_file.read().split('\n')])

    return data[['Site', 'StationName','State', 'Country', 'Latitude', 'Longitude', 'Elevation']]

def read_wind_direction(metar):
    metar_match = search(r'^[^\s]+\s\d{6}Z\s(?:AUTO\s|COR\s)?([VR0-9]{2}B?)\d?\d{2}(?:G[^K]+)?KT\s', metar)
    return (int(metar_match.group(1)) * 10 if metar_match.group(1) != 'VRB' else None) if metar_match is not None else None

def read_wind_speed(metar):
    metar_match = search(r'^[^\s]+\s\d{6}Z\s(?:AUTO\s|COR\s)?[VR0-9]{2}B?(\d?\d{2})(?:G[^K]+)?KT\s', metar)
    return (int(metar_match.group(1)) * units.knot).to(units.knot).magnitude if metar_match is not None and metar_match.group(1) is not None else None

def read_wind_gust(metar):
    metar_match = search(r'^[^\s]+\s\d{6}Z\s(?:AUTO\s|COR\s)?[VR0-9]{2}B?\d?\d{2}(?:G([^K]+))?KT\s', metar)
    return (int(metar_match.group(1)) * units.knot).to(units.knot).magnitude if metar_match is not None and metar_match.group(1) is not None else None

def process(filename, collective):
    global location_file
    if location_file is None:
        location_file = read_gempak_locations('data/misc/metar_loc.txt')
    print('{0:>11s}: {1:s}'.format('Reading', filename))
    data = read_csv(filename)
    data['time'] = to_datetime(data.valid)
    data['10_meter_wind_from_direction'] = [read_wind_direction(x) for x in data.metar]
    data['10_meter_wind_speed'] = [read_wind_speed(x) for x in data.metar]
    data['10_meter_wind_speed_of_gust'] = [read_wind_gust(x) for x in data.metar]
    data = data[['station', 'time', '10_meter_wind_from_direction', '10_meter_wind_speed', '10_meter_wind_speed_of_gust']]
    loc = [dict([(k, [x for x in v.values()][0]) for k, v in location_file.loc[location_file.Site==site].to_dict().items()]) for site in data.station.unique()]
    if collective is None:
        collective = {
            'locations': loc,
            'observations': data
        }
    else:
        collective['locations'].extend(loc)
        collective['observations'] = concat([collective['observations'], data])
    return collective

def output(path, collective, replace=False):
    if not exists(path):
        makedirs(path)
    output_file = join(path, 'locations.nc')
    print('{0:>11s}: {1:s}'.format('Writing', output_file))
    data = DataFrame(collective['locations'])
    if exists(output_file):
        data = concat([data, open_dataset(output_file).to_dataframe()])
    data = data.groupby(['Site']).first().reset_index()[['Site', 'StationName','State', 'Country', 'Latitude', 'Longitude', 'Elevation']]
    
    data.to_xarray().to_netcdf(output_file)

    observations = collective['observations']

    for site in observations.station.unique():
        data = observations[[x for x in observations.columns if x != 'station']][observations.station == site]
        site_file = join(path, site + '.nc')
        print('{0:>11s}: {1:s}'.format('Writing', site_file))
        if exists(site_file) and not replace:
            data = concat([data, open_dataset(site_file).to_dataframe()])
        data.sort_values(['time']).reset_index().to_xarray().to_netcdf(site_file)

if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv
    from os.path import split, join
    from util import process_directory

    parser = ArgumentParser('process_metar.py', description="Ingest METAR observations in CSV format from Iowa State Univeristy")
    parser.add_argument('-r', '--replace', dest='replace', action='store_true', help='Replace existing data')
    parser.add_argument('-d', '--directory', dest='directory', type=str, default='data/metars', help='Directory to process')
    opts = parser.parse_args(argv[1:])

    data_dir = opts.directory
    output_dir = join(split(data_dir)[0], 'observations')

    process_directory(data_dir, process, lambda a:output(output_dir, a, replace=opts.replace), filter_func=lambda a:[a])