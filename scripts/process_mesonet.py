"""
Process Mesonet observations from Synoptic Data
"""
from re import search
from pandas import read_csv, to_datetime, concat, DataFrame
from os.path import join, exists
from os import makedirs
from xarray import open_dataset
from io import StringIO
from pint import Quantity, UnitRegistry
from datetime import datetime

units = UnitRegistry()

def process(filename, collective):
    print('{0:>11s}: {1:s}'.format('Reading', filename))
    with open(filename, 'r') as in_file:
        data = in_file.read()
    loc = dict(zip(['Site', 'StationName','State', 'Country', 'Latitude', 'Longitude', 'Elevation'],
                   [search('STATION[^\:]*\:\s*([^\n]+)', data).group(1),
                    search('STATION NAME[^\:]*\:\s*([^\n]+)', data).group(1),
                    search('STATE[^\:]*\:\s*([^\n]+)', data).group(1),
                    'US',
                    float(search('LATITUDE[^\:]*\:\s*([^\n]+)', data).group(1)),
                    float(search('LONGITUDE[^\:]*\:\s*([^\n]+)', data).group(1)),
                    (float(search('ELEVATION[^\:]*\:\s*([^\n]+)', data).group(1)) * units('ft')).to(units('m')).magnitude]) )
    columns = search('Station_ID[^\n]+',data).group(0).split(',')
    src_units  = dict([(columns[i], units(x.lower().replace('%','').replace('code', '').replace('w', 'watt'))) for i,x in enumerate(search(',,[^\n]+', data).group(0).split(','))])

    dest_units = {'Station_ID': units(''), 'Date_Time': units(''), 'altimeter_set_1': units('pascal'), 'air_temp_set_1': units('K'), 
                'relative_humidity_set_1': units(''), 'wind_speed_set_1': units('m s**-1'), 'wind_direction_set_1': units('degree'), 
                'wind_gust_set_1': units('m s**-1'), 'precip_accum_since_local_midnight_set_1': units('mm'), 'dew_point_temperature_set_1d': units('K'), 
                'pressure_set_1d': units('pascal'), 'sea_level_pressure_set_1d': units('pascal'), 'precip_accum_set_1': units('mm')}
    cf_columns = {'Station_ID': 'station', 'Date_Time': 'time', 'altimeter_set_1': 'altimeter_pressure', 'air_temp_set_1': '2_meter_air_temperature', 
                'relative_humidity_set_1': '2_meter_relative_humidity', 'wind_speed_set_1': '10_meter_wind_speed', 'wind_direction_set_1': '10_meter_wind_from_direction', 
                'wind_gust_set_1': '10_meter_wind_speed_of_gust', 'precip_accum_since_local_midnight_set_1': 'precipitation_amount', 'precip_accum_set_1': 'precipitation_amount',
                'dew_point_temperature_set_1d': '2_meter_dew_point_temperature', 'pressure_set_1d': 'surface_air_pressure', 
                'sea_level_pressure_set_1d': 'air_pressure_at_mean_sea_level'}
    data = read_csv(StringIO('\n'.join([x for x in data.split('\n') if len(x)>0 and x[0] != '#' and x[0:2] != ',,'])))
    data = data[[col for col in data.columns if col in cf_columns]]
    print(data)
    for col in data.columns:
        data[col] = Quantity(data[col].to_numpy(), src_units[col]).to(dest_units[col]).magnitude
    data.columns = [cf_columns[x] for x in data.columns]
    data['time'] = [to_datetime(datetime.strptime(data['time'][i], '%Y-%m-%dT%H:%M:%SZ')) for i in data.index]
    if collective is None:
        collective = {
            'locations': [loc],
            'observations': data
        }
    else:
        collective['locations'].append(loc)
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

    parser = ArgumentParser('process_mesonet.py', description="Ingest mesonet observations in CSV format from Synoptic Labs")
    parser.add_argument('-r', '--replace', dest='replace', action='store_true', help='Replace existing data')
    parser.add_argument('-d', '--directory', dest='directory', type=str, default='data/mesonet', help='Directory to process')
    opts = parser.parse_args(argv[1:])

    data_dir = opts.directory
    output_dir = join(split(data_dir)[0], 'observations')

    process_directory(data_dir, process, lambda a:output(output_dir, a, replace=opts.replace), filter_func=lambda a:[a])