"""
Process METARs from Iowa State - Iowa Enviromental Mesonet (https://mesonet.agron.iastate.edu/request/download.phtml)
"""
from re import search
from pandas import read_csv, to_datetime, concat
from os.path import join, exists
from pint import UnitRegistry
from xarray import open_dataset

units = UnitRegistry()

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
    data = read_csv(filename)
    data['time'] = to_datetime(data.valid)
    data['10_meter_wind_from_direction'] = [read_wind_direction(x) for x in data.metar]
    data['10_meter_wind_speed'] = [read_wind_speed(x) for x in data.metar]
    data['10_meter_wind_speed_of_gust'] = [read_wind_gust(x) for x in data.metar]
    data = data[['station', 'time', '10_meter_wind_from_direction', '10_meter_wind_speed', '10_meter_wind_speed_of_gust']]
    return concat([data, collective]) if collective is not None else data

def output(path, collective):
    for site in collective.station.unique():
        data = collective[['time', '10_meter_wind_from_direction', '10_meter_wind_speed', '10_meter_wind_speed_of_gust']][collective.station == site]
        site_file = join(path, site + '.nc')
        previous = None
        if exists(site_file):
            data = concat([data, open_dataset(site_file).to_dataframe()])
        data.sort_values(['time']).reset_index(drop=True).to_xarray().to_netcdf(site_file)

if __name__ == '__main__':
    from util import process_directory
    data_dir = 'data/metars'
    process_directory(data_dir, process, lambda a:output(data_dir, a), filter_func=lambda a:[a])