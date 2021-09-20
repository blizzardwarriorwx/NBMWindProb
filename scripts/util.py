from os import listdir, makedirs
from os.path import join, split, exists, getsize
from shutil import move
from pyproj import Proj
from cftime import date2num
from numpy import arange, where, array, uint16, uint32, isfinite, meshgrid, round, NaN, ones, uint8, power, sqrt, sum
from pygrib import open as open_grib
from multiprocessing import Queue, Process
from queue import Empty
from pandas import merge, to_timedelta
from netCDF4 import Dataset as NCDataset
from xarray import open_dataset, Dataset as XRDataset
from pint import UnitRegistry
from pyproj import CRS
from pyproj.transformer import Transformer

wgs84 = CRS(4326)
units = UnitRegistry()

nbm_forecast_hours = list(range(1,37,1)) + list(range(39, 169, 3))

nbm_fields = [
    {
        'variable_name': '10_meter_wind_speed_of_gust_mean',
        'field_id': 'wind_speed_of_gust',
        'grib_id': {
            'name': 'Wind speed (gust)',
            'level': 10,
            'typeOfLevel': 'heightAboveGround',
            'derivedForecast': None
        }
    },
    {
        'variable_name': '10_meter_wind_speed_of_gust_standard_deviation',
        'field_id': 'wind_speed_of_gust',
        'grib_id': {
            'name': 'Wind speed (gust)',
            'level': 10,
            'typeOfLevel': 'heightAboveGround',
            'derivedForecast': 2
        }
    },
    {
        'variable_name': '10_meter_wind_speed_mean',
        'field_id': 'wind_speed',
        'grib_id': {
            'name': '10 metre wind speed',
            'level': 10,
            'typeOfLevel': 'heightAboveGround',
            'derivedForecast': None
        }
    },
    {
        'variable_name': '10_meter_wind_speed_standard_deviation',
        'field_id': 'wind_speed',
        'grib_id': {
            'name': '10 metre wind speed',
            'level': 10,
            'typeOfLevel': 'heightAboveGround',
            'derivedForecast': 2
        }
    },
    {
        'variable_name': '10_meter_wind_from_direction_mean',
        'field_id': 'wind_from_direction',
        'grib_id': {
            'name': '10 metre wind direction',
            'level': 10,
            'typeOfLevel': 'heightAboveGround',
            'derivedForecast': None
        }
    },
    {
        'variable_name': '30_meter_wind_speed_mean',
        'field_id': 'wind_speed',
        'grib_id': {
            'name': 'Wind speed',
            'level': 30,
            'typeOfLevel': 'heightAboveGround',
            'derivedForecast': None
        }
    },
    {
        'variable_name': '80_meter_wind_speed_mean',
        'field_id': 'wind_speed',
        'grib_id': {
            'name': 'Wind speed',
            'level': 80,
            'typeOfLevel': 'heightAboveGround',
            'derivedForecast': None
        }
    },
    {
        'variable_name': '30_meter_wind_from_direction_mean',
        'field_id': 'wind_from_direction',
        'grib_id': {
            'name': 'Wind direction',
            'level': 30,
            'typeOfLevel': 'heightAboveGround',
            'derivedForecast': None
        }
    },
    {
        'variable_name': '80_meter_wind_from_direction_mean',
        'field_id': 'wind_from_direction',
        'grib_id': {
            'name': 'Wind direction',
            'level': 80,
            'typeOfLevel': 'heightAboveGround',
            'derivedForecast': None
        }
    },
    {
        'variable_name': '2_meter_maximum_air_temperature_mean',
        'field_id': 'air_temperature',
        'grib_id': {
            'name': 'Maximum temperature',
            'level': 2,
            'typeOfLevel': 'heightAboveGround',
            'derivedForecast': None
        }
    },
    {
        'variable_name': '2_meter_maximum_air_temperature_standard_deviation',
        'field_id': 'air_temperature',
        'grib_id': {
            'name': 'Maximum temperature',
            'level': 2,
            'typeOfLevel': 'heightAboveGround',
            'derivedForecast': 2
        }
    },
    {
        'variable_name': '2_meter_minimum_air_temperature_mean',
        'field_id': 'air_temperature',
        'grib_id': {
            'name': 'Minimum temperature',
            'level': 2,
            'typeOfLevel': 'heightAboveGround',
            'derivedForecast': None
        }
    },
    {
        'variable_name': '2_meter_minimum_air_temperature_standard_deviation',
        'field_id': 'air_temperature',
        'grib_id': {
            'name': 'Minimum temperature',
            'level': 2,
            'typeOfLevel': 'heightAboveGround',
            'derivedForecast': 2
        }
    },
    {
        'variable_name': '2_meter_air_temperature_mean',
        'field_id': 'air_temperature',
        'grib_id': {
            'name': '2 metre temperature',
            'level': 2,
            'typeOfLevel': 'heightAboveGround',
            'derivedForecast': None
        }
    },
    {
        'variable_name': '2_meter_air_temperature_standard_deviation',
        'field_id': 'air_temperature',
        'grib_id': {
            'name': '2 metre temperature',
            'level': 2,
            'typeOfLevel': 'heightAboveGround',
            'derivedForecast': 2
        }
    },
    {
        'variable_name': 'mixing_height_mean',
        'field_id': 'mixing_height',
        'grib_id': {
            'name': 'Mixed layer depth',
            'nameOfFirstFixedSurface': 'Entire atmosphere',
            'unitsOfFirstFixedSurface': 'considered as a single layer',
            'derivedForecast': None
        }
    },
    {
        'variable_name': 'atmosphere_convective_available_potential_energy_wrt_surface_mean',
        'field_id': 'atmosphere_convective_available_potential_energy_wrt_surface',
        'grib_id': {
            'name': 'Convective available potential energy',
            'level': 0,
            'typeOfLevel': 'surface',
            'derivedForecast': None
        }
    },
    {
        'variable_name': 'atmosphere_convective_available_potential_energy_wrt_surface_standard_deviation',
        'field_id': 'atmosphere_convective_available_potential_energy_wrt_surface',
        'grib_id': {
            'name': 'Convective available potential energy',
            'level': 0,
            'typeOfLevel': 'surface',
            'derivedForecast': 2
        }
    }
]

nbm_analysis_fields = [x for x in nbm_fields if x['variable_name'] not in ['2_meter_minimum_air_temperature_standard_deviation',
                                                                           '2_meter_minimum_air_temperature_mean',
                                                                           '2_meter_maximum_air_temperature_standard_deviation',
                                                                           '2_meter_maximum_air_temperature_mean']]

rtma_fields = [
    {
        'variable_name': '2_meter_dew_point_temperature',
        'field_id': 'dew_point_temperature',
        'grib_id': {
            'name': '2 metre dewpoint temperature',
            'typeOfGeneratingProcess': 0
        }
    },
    {
        'variable_name': 'precipitation_amount',
        'field_id': 'precipitation_amount',
        'grib_id': {
            'name': 'Total Precipitation', 
            'typeOfGeneratingProcess': 2
        }
    },
    {
        'variable_name': 'surface_air_pressure',
        'field_id': 'surface_air_pressure',
        'grib_id': {
            'name': 'Surface pressure', 
            'typeOfGeneratingProcess': 0
        }
    },
    {
        'variable_name': '2_meter_air_temperature',
        'field_id': 'air_temperature',
        'grib_id': {
            'name': '2 metre temperature', 
            'typeOfGeneratingProcess': 0
        }
    },
    {
        'variable_name': 'elevation',
        'field_id': 'geopotential_height',
        'grib_id': {
            'name': 'Orography', 
            'typeOfGeneratingProcess': 0
        }
    },
    {
        'variable_name': '10_meter_wind_from_direction',
        'field_id': 'wind_from_direction',
        'grib_id': {
            'name': '10 metre wind direction',
            'typeOfGeneratingProcess': 0
        }
    },
    {
        'variable_name': '10_meter_wind_speed',
        'field_id': 'wind_speed',
        'grib_id': {
            'name': '10 metre wind speed', 
            'typeOfGeneratingProcess': 0
        }
    },
    {
        'variable_name': '10_meter_wind_speed_of_gust',
        'field_id': 'wind_speed_of_gust',
        'grib_id': {
            'name': 'Wind speed (gust)', 
            'typeOfGeneratingProcess': 0
        }
    }
]

field_defs = {
    'wind_speed_of_gust'   : (uint16, 0,    0.1,  'm s**-1'),
    'wind_speed'           : (uint16, 0,    0.1,  'm s**-1'),
    'wind_from_direction'  : (uint16, 0,    1,    'degree'),
    'dew_point_temperature': (uint16, 0,    0.01, 'K'),
    'precipitation_amount' : (uint16, 0,    0.1,  'kg m-2'),
    'surface_air_pressure' : (uint32, 0,    1,    'Pa'),
    'air_temperature'      : (uint16, 0,    0.01, 'K'),
    'geopotential_height'  : (uint16, -100, 0.1,  'm'),
    'mixing_height'        : (uint16, -100, 0.1,  'm'),
    'atmosphere_convective_available_potential_energy_wrt_surface' : (uint16, 0, 1, 'J kg**-1')
}

def latlon2ij(ds, lat, lon):
    prj = Proj(ds.lambert_conformal_conic.proj4params)
    x0 = ds.x.values[0]
    y0 = ds.y.values[0]
    dx = ds.x.values[1] - x0
    dy = ds.y.values[1] - y0
    x, y = prj(lon, lat)
    return int(round((x - x0) / dx)), int(round((y - y0) / dy))

def xy2latlon(ds, x, y):
    prj = Proj(ds.lambert_conformal_conic.proj4params)
    lon, lat = prj(x, y, inverse=True)
    return lat, lon

def distance(lon1, lat1, lon2, lat2):
    return sqrt(sum([power(i, 2) for i in Proj(' '.join(['+{0}={1}'.format(k, v) for k, v in {'a': 6371200.0, 'b': 6371200.0, 'proj': 'aeqd', 'lon_0': lon1, 'lat_0': lat1}.items()]))(lon2, lat2)]))

def process_directory(path, on_each, on_final, filter_func=None):
    if not exists(join(path, 'processed')):
        makedirs(join(path, 'processed'))
    raw_files = sorted([join(path, 'incoming', x) for x in listdir(join(path, 'incoming')) if x[0] != '.'])
    if filter_func is None:
        raw_files = [[x] for x in raw_files]
    else:
        raw_files = filter_func(raw_files)
    for group in raw_files:
        arg = None
        for filename in group:
            arg = on_each(filename, arg)
            move(filename, join(path, 'processed', split(filename)[1]))
        if arg is not None:
            on_final(arg)

def process_directory_parallel(path, process_count, on_each, on_final, on_each_args=[], on_final_args=[], filter_func=None):
    raw_files = sorted([join(path, 'incoming', x) for x in listdir(join(path, 'incoming')) if x[0] != '.'])
    if filter_func is None:
        raw_files = [[x] for x in raw_files]
    else:
        raw_files = filter_func(raw_files)
    if not exists(join(path, 'processed')):
        makedirs(join(path, 'processed'))
    raw_files = [(x, on_each, on_final, on_each_args, on_final_args) for x in raw_files]
    p = ProcessPool(process_count)
    p.map(process_file_parallel, raw_files)

def process_file_parallel(group, on_each, on_final, on_each_args=[], on_final_args=[], output_function=print):
    arg = None
    for filename in group:
        arg = on_each(filename, arg, *on_each_args, output_function=output_function)
        path, file_part = split(filename)
        path = split(path)[0]
        move(filename, join(path, 'processed', file_part))
    if arg is not None:
        on_final(arg, *on_final_args, output_function=output_function)

def matches_fields(msg, fields):
    return max([min([(msg[k] if k in msg.keys() else None) == v for k,v in f['grib_id'].items()]) for f in fields])

def get_variable_info(msg, fields):
    return [f for f in fields if min([(msg[k] if k in msg.keys() else None) == v for k,v in f['grib_id'].items()])][0]

def file_exists(dir, file):
    return (getsize(join(dir, 'incoming', file)) > 0 if exists(join(dir, 'incoming', file)) else False) or (getsize(join(dir, 'processed', file)) > 0 if exists(join(dir, 'processed', file)) else False)

class ProcessPool(object):
    @staticmethod
    def multiprint(i,pid, msg):
        print('\x1B[1000D\x1B[{0:d}A\x1B[K({2:02d}) {3:s}\x1B[{1:d}B'.format(i+2,i+1,pid,str(msg)))
    @staticmethod
    def process_action(pool_size, pid, queue, action):
        try:
            arg = queue.get(timeout=0.01)
            while True:
                action(*arg, output_function=lambda a: ProcessPool.multiprint(pool_size-pid-1,pid, a))
                arg = queue.get(timeout=0.01)
        except Empty:
            pass
    def __init__(self, size):
        self.size = size
    def map(self, function, args):
        self.queue = Queue()
        [self.queue.put(x) for x in args]
        [print() for i in range(self.size+1)]
        self.processes = [Process(target=ProcessPool.process_action, args=(self.size, i, self.queue, function)) for i in range(self.size)]
        [x.start() for x in self.processes]
        self.processes[0].join()

def numberToBase(n, b):
    if n == 0:
        return [0]
    digits = []
    while n:
        digits.append(int(n % b))
        n //= b
    return digits[::-1]

def encode(lat, lon):
    proj = Proj({'a': 6371200.0, 'b': 6371200.0, 'proj': 'lcc', 'lon_0': 265.0, 'lat_0': 25.0, 'lat_1': 25.0, 'lat_2': 25.0})
    x0 = -2763204.499231994
    y0 =  -263789.4687076054
    dx = 2539.703
    dy = 2539.703
    x, y = proj(lon, lat)
    x = ''.join([list('23456789CFGHJMPQRVWX')[i] for i in numberToBase(int(round((x-x0) / dx)), 20)])
    y = ''.join([list('23456789CFGHJMPQRVWX')[i] for i in numberToBase(int(round((y-y0) / dy)), 20)])
    return '{0:>0s}+{1:>0s}'.format(x,y)

def decode(code):
    x, y = code.split('+')
    x = sum([20**(len(x)-1-j)*int('23456789CFGHJMPQRVWX'.find(i)) for j,i in enumerate(x)])
    y = sum([20**(len(y)-1-j)*int('23456789CFGHJMPQRVWX'.find(i)) for j,i in enumerate(y)])
    return x, y

def link_observations(obs_file, grid_file, value_func=lambda a:a, obs_tag='obs', grid_tag='grid'):
    observation_data = open_dataset(obs_file).to_dataframe()
    for field in [x for x in observation_data.columns if x.find('10_meter_wind_speed') > -1]:
        observation_data[field] = value_func((array(observation_data[field]) * units('m s**-1')).to(units('knots')).magnitude)
    observation_data = observation_data[isfinite(observation_data['10_meter_wind_speed'])]
    observation_data.time = observation_data.time.dt.round('H')
    observation_data = observation_data.groupby(['time']).max().reset_index()
    observation_data.columns = [x + '_{0:s}'.format(obs_tag) for x in observation_data.columns]

    grid_data = open_dataset(grid_file).to_dataframe()
    for field in [x for x in grid_data.columns if x.find('10_meter_wind_speed') > -1]:
        grid_data[field] = value_func((array(grid_data[field]) * units('m s**-1')).to(units('knots')).magnitude)
    grid_data['time'] = [grid_data.reference_time[i] + to_timedelta('{0:.0f}H'.format(grid_data.time_since_reference[i])) for i in grid_data.index]
    grid_data.columns = [x + '_{0:s}'.format(grid_tag) for x in grid_data.columns]

    return merge(observation_data, grid_data, how='inner', left_on='time_{0:s}'.format(obs_tag), right_on='time_{0:s}'.format(grid_tag)).reset_index()

def link_grid(obs_file, grid_file, value_func=lambda a:a, obs_tag='obs', grid_tag='grid'):
    observation_data = open_dataset(obs_file).to_dataframe()
    for field in [x for x in observation_data.columns if x.find('10_meter_wind_speed') > -1]:
        observation_data[field] = value_func((array(observation_data[field]) * units('m s**-1')).to(units('knots')).magnitude)
    observation_data['time'] = [observation_data.reference_time[i] + to_timedelta('{0:.0f}H'.format(observation_data.time_since_reference[i])) for i in observation_data.index]
    observation_data.columns = [x + '_{0:s}'.format(obs_tag) for x in observation_data.columns]

    grid_data = open_dataset(grid_file).to_dataframe()
    for field in [x for x in grid_data.columns if x.find('10_meter_wind_speed') > -1]:
        grid_data[field] = value_func((array(grid_data[field]) * units('m s**-1')).to(units('knots')).magnitude)
    grid_data['time'] = [grid_data.reference_time[i] + to_timedelta('{0:.0f}H'.format(grid_data.time_since_reference[i])) for i in grid_data.index]
    grid_data.columns = [x + '_{0:s}'.format(grid_tag) for x in grid_data.columns]

    return merge(observation_data, grid_data, how='inner', left_on='time_{0:s}'.format(obs_tag), right_on='time_{0:s}'.format(grid_tag)).reset_index()

class GridArrayNetCDF(object):
    grid_maps = {}
    def __init__(self, filename, fields, mode=None, compress=True, forecast_hours=nbm_forecast_hours, analysis_time=lambda a:a):
        if mode is None:
            if exists(filename):
                mode = 'a'
            else:
                mode = 'w'
        self.dataset = NCDataset(filename, mode, format='NETCDF4')
        self.analysis_time = analysis_time
        self.forecast_hours = array(forecast_hours)
        self.compress = compress
        self.fields = fields
        self.chunking = (1, 1, 51, 55)
        self.proj = {'a': 6371200.0, 'b': 6371200.0, 'proj': 'lcc', 'lon_0': 265.0, 'lat_0': 25.0, 'lat_1': 25.0, 'lat_2': 25.0}
        self.x0 = -2763204.499231994
        self.y0 = 1420033.6202923944
        self.nx = 1100
        self.ny = 714
        self.dx = 2539.703
        self.dy = 2539.703
        self.reference_time_units = 'hours since 2020-01-01 00:00:00.0'
        self.reference_time_calendar = 'gregorian'
        self.reference_time = None
        self.__init_database__()
    def __init_database__(self):
        compression_args = {}

        self.dataset.createDimension('reference_time', 1)
        self.dataset.createDimension('time_since_reference', len(self.forecast_hours))    
        self.dataset.createDimension('y', self.ny)
        self.dataset.createDimension('x', self.nx)
        
        compression_args = {
            'zlib': self.compress,
            'complevel': 9,
            'shuffle': True,
        }

        lambert_conformal_conic = self.dataset.createVariable('lambert_conformal_conic', 'uint8', (), zlib=self.compress)
        lambert_conformal_conic.grid_mapping_name = 'lambert_conformal_conic'
        lambert_conformal_conic.standard_parallel = [self.proj['lat_1'], self.proj['lat_2']]
        lambert_conformal_conic.longitude_of_central_meridian = self.proj['lon_0']
        lambert_conformal_conic.latitude_of_projection_origin = self.proj['lat_0']
        lambert_conformal_conic.earth_radius = self.proj['a']
        lambert_conformal_conic._CoordinateTransformType = 'Projection'
        lambert_conformal_conic._CoordinateAxisTypes = 'GeoX GeoY'
        lambert_conformal_conic.proj4params = ' '.join(['+{0}={1}'.format(k, v) for k, v in self.proj.items()])

        cycle = self.dataset.createVariable('reference_time', uint32, ('reference_time'), fill_value=uint32(-1), chunksizes=self.chunking[0:1], **compression_args)
        cycle.standard_name = 'forecast_reference_time'
        cycle.long_name = 'forecast_reference_time'
        cycle.units = self.reference_time_units
        cycle.calendar = self.reference_time_calendar

        forecast_hour = self.dataset.createVariable('time_since_reference', uint16, ('time_since_reference'), fill_value=uint16(-1), chunksizes=self.chunking[1:2], **compression_args)
        forecast_hour.standard_name = 'forecast_period'
        forecast_hour.long_name = 'forecast_period'
        forecast_hour[:] = self.forecast_hours

        y = self.dataset.createVariable('y', uint32, ('y'), fill_value=uint32(-1), chunksizes=self.chunking[2:3], contiguous=False, **compression_args)
        y.scale_factor = self.dy
        y.add_offset = self.y0
        y.long_name = 'projection_y_coordinate'
        y.standard_name = 'projection_y_coordinate'
        y.units = 'm'
        y._CoordinateAxisType = 'GeoY'
        y[:] = self.dy * arange(self.ny) + self.y0

        x = self.dataset.createVariable('x', uint32, ('x'), fill_value=uint32(-1), chunksizes=self.chunking[3:4], contiguous=False, **compression_args)
        x.scale_factor = self.dx
        x.add_offset = self.x0
        x.long_name = 'projection_x_coordinate'
        x.standard_name = 'projection_x_coordinate'
        x.units = 'm'
        x._CoordinateAxisType = 'GeoX'
        x[:] = self.dx * arange(self.nx) + self.x0

        for field in self.fields:
            field_def = field_defs[field['field_id']]
            field_var = self.dataset.createVariable(field['variable_name'], field_def[0], ('reference_time', 'time_since_reference', 'y', 'x'), fill_value=field_def[0](-1), chunksizes=self.chunking, contiguous=False, **compression_args)
            field_var.long_name = field['variable_name']
            field_var.standard_name = field['field_id']
            field_var.scale_factor = field_def[2]
            field_var.add_offset = field_def[1]
            field_var.units = field_def[3]
            field_var.grid_mapping = 'lambert_conformal_conic'
        
    def close(self, output_function=print):
        self.dataset.close()
    def append(self, filename, force_dtype=None, output_function=print):
        grb = open_grib(filename)
        for i in range(grb.messages):
            msg = grb.message(i + 1)
            if matches_fields(msg, self.fields):
                m = n = None
                source_key = (msg.centre, msg.subCentre, msg.generatingProcessIdentifier)
                if 'x' not in self.dataset.variables:
                    self.__init_database__(msg)
                field_info = get_variable_info(msg, self.fields)
                output_function('{0:>11s}: {1:s} - {2:s}'.format('Adding', filename, field_info['variable_name']))

                if source_key in GridArrayNetCDF.grid_maps:
                    m, n = GridArrayNetCDF.grid_maps[source_key]
                else:
                    msg_crs = CRS.from_dict(msg.projparams)
                    crs_projection = Transformer.from_crs(wgs84, msg_crs, always_xy=True)
                    msg_lat, msg_lon = msg.latlons()
                    msg_x, msg_y = crs_projection.transform(msg_lon, msg_lat)
                    msg_x0 = msg_x[0,0]
                    msg_y0 = msg_y[0,0]
                    msg_dx = msg_x[0,1] - msg_x0
                    msg_dy = msg_y[1,0] - msg_y0
                    msg_grid_type = msg.gridType
                    projection = Transformer.from_crs(CRS.from_dict(self.proj), msg_crs, always_xy=True)
                    x, y = meshgrid(self.x0 + self.dx * arange(self.nx),
                                    self.y0 + self.dy * arange(self.ny))
                    px, py = projection.transform(x, y)
                    if msg_grid_type == 'regular_ll':
                        px %= 360
                    m = round((px - msg_x0) / msg_dx).astype('int64')
                    n = round((py - msg_y0) / msg_dy).astype('int64')
                    GridArrayNetCDF.grid_maps[source_key] = (m, n)

                if self.reference_time is None:
                    self.reference_time = self.analysis_time(msg.analDate)
                    self.dataset.variables['reference_time'][0] = date2num(self.reference_time, units=self.reference_time_units, calendar=self.reference_time_calendar)
                forecast_hour = int((msg.validDate - self.reference_time).total_seconds()/3600)
                forecast_hour_index = ([y for x in where(self.forecast_hours==forecast_hour) for y in x] + [None])[0]
                field_var = self.dataset.variables[field_info['variable_name']]
                if forecast_hour_index is not None:
                    field_var[0,forecast_hour_index,:,:] = msg.values[n, m]
        grb.close()

class GridArray(object):
    grid_maps = {}
    def __init__(self, filename, fields, mode=None, compress=True, forecast_hours=nbm_forecast_hours, analysis_time=lambda a:a):
        self.filename = filename
        self.compress = compress
        self.analysis_time = analysis_time
        self.forecast_hours = array(forecast_hours)
        self.fields = fields
        self.chunking = (1, 1, 51, 55)
        self.proj = {'a': 6371200.0, 'b': 6371200.0, 'proj': 'lcc', 'lon_0': 265.0, 'lat_0': 25.0, 'lat_1': 25.0, 'lat_2': 25.0}
        self.x0 = -2763204.499231994
        self.y0 = 1420033.6202923944
        self.nx = 1100
        self.ny = 714
        self.dx = 2539.703
        self.dy = 2539.703
        self.reference_time_units = 'hours since 2020-01-01 00:00:00.0'
        self.reference_time_calendar = 'gregorian'
        self.reference_time = None
        self.variables = {}
    def append(self, filename, force_dtype=None, output_function=print):
        grb = open_grib(filename)
        for i in range(grb.messages):
            msg = grb.message(i+1)
            if matches_fields(msg, self.fields):
                m = n = None
                source_key = (msg.centre, msg.subCentre, msg.generatingProcessIdentifier)
                field_info = get_variable_info(msg, self.fields)
                output_function('{0:>11s}: {1:s} - {2:s}'.format('Adding', filename, field_info['variable_name']))
                if field_info['variable_name'] not in self.variables:
                    field_def = field_defs[field_info['field_id']]
                    self.variables[field_info['variable_name']] = (
                        ['reference_time', 'time_since_reference', 'y', 'x'], 
                        ones((1, len(self.forecast_hours), self.ny, self.nx), dtype=(msg.values.dtype if force_dtype is None else force_dtype)) * NaN, 
                        {
                            'long_name': field_info['variable_name'],
                            'standard_name': field_info['field_id'],
                            'units': field_def[3],
                            'grid_mapping': 'lambert_conformal_conic'
                        }
                    )
                if source_key in GridArray.grid_maps:
                    m, n = GridArray.grid_maps[source_key]
                else:
                    msg_crs = CRS.from_dict(msg.projparams)
                    crs_projection = Transformer.from_crs(wgs84, msg_crs, always_xy=True)
                    msg_lat, msg_lon = msg.latlons()
                    msg_x, msg_y = crs_projection.transform(msg_lon, msg_lat)
                    msg_x0 = msg_x[0,0]
                    msg_y0 = msg_y[0,0]
                    msg_dx = msg_x[0,1] - msg_x0
                    msg_dy = msg_y[1,0] - msg_y0
                    msg_grid_type = msg.gridType
                    projection = Transformer.from_crs(CRS.from_dict(self.proj), msg_crs, always_xy=True)
                    x, y = meshgrid(self.x0 + self.dx * arange(self.nx),
                                    self.y0 + self.dy * arange(self.ny))
                    px, py = projection.transform(x, y)
                    if msg_grid_type == 'regular_ll':
                        px %= 360
                    m = round((px - msg_x0) / msg_dx).astype('int64')
                    n = round((py - msg_y0) / msg_dy).astype('int64')
                    GridArray.grid_maps[source_key] = (m, n)
                if self.reference_time is None:
                    analDate = self.analysis_time(msg.analDate)
                    self.reference_time = analDate
                forecast_hour = int((msg.validDate - self.reference_time).total_seconds()/3600)
                forecast_hour_index = ([y for x in where(self.forecast_hours==forecast_hour) for y in x] + [None])[0]
                if forecast_hour_index is not None:
                    self.variables[field_info['variable_name']][1][0, forecast_hour_index, :, :] = msg.values[n, m]
        grb.close()
    def to_xarray(self):
        if type(self.variables) == dict:
            for field_info in self.fields:
                if field_info['variable_name'] not in self.variables:
                    field_def = field_defs[field_info['field_id']]
                    self.variables[field_info['variable_name']] = (
                        ['reference_time', 'time_since_reference', 'y', 'x'], 
                        ones((1, len(self.forecast_hours), self.ny, self.nx), dtype='float32') * NaN, 
                        {
                            'long_name': field_info['variable_name'],
                            'standard_name': field_info['field_id'],
                            'units': field_def[3],
                            'grid_mapping': 'lambert_conformal_conic'
                        }
                    )
            self.variables['lambert_conformal_conic'] = (
                [],
                uint8(0),
                {
                    'grid_mapping_name': 'lambert_conformal_conic',
                    'standard_parallel': [self.proj['lat_1'], self.proj['lat_2']],
                    'longitude_of_central_meridian': self.proj['lon_0'],
                    'latitude_of_projection_origin': self.proj['lat_0'],
                    'earth_radius': self.proj['a'],
                    '_CoordinateTransformType': 'Projection',
                    '_CoordinateAxisTypes': 'GeoX GeoY',
                    'proj4params': ' '.join(['+{0}={1}'.format(k, v) for k, v in self.proj.items()])
                }
            )
            self.variables = XRDataset(
                self.variables,
                coords={
                    'reference_time':(
                        ['reference_time'],
                        [date2num(self.reference_time, self.reference_time_units, calendar=self.reference_time_calendar)],
                        {
                            'standard_name': 'forecast_reference_time',
                            'long_name': 'forecast_reference_time',
                            'units': self.reference_time_units,
                            'calendar': self.reference_time_calendar
                        }
                    ),
                    'time_since_reference': (
                        ['time_since_reference'],
                        self.forecast_hours,
                        {
                            'standard_name': 'forecast_period',
                            'long_name': 'forecast_period'
                        }
                    ),
                    'y': (
                        ['y'],
                        self.dy * arange(self.ny) + self.y0,
                        {
                            'long_name': 'projection_y_coordinate',
                            'standard_name': 'projection_y_coordinate',
                            'units': 'm',
                            '_CoordinateAxisType': 'GeoY',
                        }
                    ),
                    'x': (
                        ['x'],
                        self.dx * arange(self.nx) + self.x0,
                        {
                            'long_name': 'projection_x_coordinate',
                            'standard_name': 'projection_x_coordinate',
                            'units': 'm',
                            '_CoordinateAxisType': 'GeoX',
                        }
                    )
                }
            )
        return self.variables
    def to_netcdf(self, xarray_data=None):
        if xarray_data is None:
            xarray_data = self.to_xarray()
        
        xarray_data.to_netcdf(self.filename,encoding=dict(
            [(f['variable_name'], {'zlib': self.compress, 'complevel': 9, 'shuffle': True, '_FillValue': field_defs[f['field_id']][0](-1), 'dtype': field_defs[f['field_id']][0], 'scale_factor': field_defs[f['field_id']][2], 'add_offset': field_defs[f['field_id']][1], 'chunksizes': self.chunking}) for f in self.fields]
          + [('reference_time', {'zlib': self.compress, 'complevel': 9, 'shuffle': True, '_FillValue': uint32(-1), 'dtype': uint32, 'chunksizes': self.chunking[0:1]}), 
             ('time_since_reference', {'zlib': self.compress, 'complevel': 9, 'shuffle': True, '_FillValue': uint16(-1), 'dtype': uint16, 'chunksizes': self.chunking[1:2]}), 
             ('y', {'zlib': self.compress, 'complevel': 9, 'shuffle': True, '_FillValue': uint32(-1), 'dtype': uint32, 'scale_factor': self.dy, 'add_offset': self.y0, 'chunksizes': self.chunking[2:3]}), 
             ('x', {'zlib': self.compress, 'complevel': 9, 'shuffle': True, '_FillValue': uint32(-1), 'dtype': uint32, 'scale_factor': self.dx, 'add_offset': self.x0, 'chunksizes': self.chunking[3:4]})]
        ))
    def close(self, output_function=print):
        output_function('{0:>11s}: {1:s}'.format('Writing', self.filename))
        self.to_netcdf()