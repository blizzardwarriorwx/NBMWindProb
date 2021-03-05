from os import listdir
from os.path import join, split, exists, getsize
from shutil import move
from netCDF4 import Dataset
from pyproj import Proj
from cftime import date2num, num2pydate
from numpy import arange, sqrt, where, array, ceil, uint16, uint32
from numpy.ma import is_masked
from pygrib import open as open_grib
from multiprocessing import Queue, Process
from queue import Empty

# def hour2num(hour):
#     return hour-1 if hour <= 36 else int((hour - 36) / 3 + 35) if hour <= 192 else int((hour - 192) / 6 + 87)

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

class GridArchive(object):
    def __init__(self, filename, fields, mode=None, compress=False, forecast_hours=nbm_forecast_hours):
        if mode is None:
            if exists(filename):
                mode = 'a'
            else:
                mode = 'w'
        self.dataset = Dataset(filename, mode, format='NETCDF4')
        self.forecast_hours = array(forecast_hours)
        self.compress = compress
        self.fields = fields
        self.chunking = (1, 1, 51, 55)
        self.proj = {'a': 6371200.0, 'b': 6371200.0, 'proj': 'lcc', 'lon_0': 265.0, 'lat_0': 25.0, 'lat_1': 25.0, 'lat_2': 25.0}
        self.x0 = -2763204.499231994
        self.y0 =  -263789.4687076054
        self.nx = 2145
        self.ny = 1377
        self.dx = 2539.703
        self.dy = 2539.703
        self.__init_database__()
        # self.nx = None
        # self.ny = None
        # self.x0 = None
        # self.y0 = None
        # self.dx = None
        # self.dy = None
        # if 'x' in self.dataset.variables:
        #     self.nx = self.dataset.variables['x'].shape[0]
        #     self.ny = self.dataset.variables['y'].shape[0]
        #     self.x0 = self.dataset.variables['x'][0]
        #     self.y0 = self.dataset.variables['y'][0]
        #     self.dx = self.dataset.variables['x'][1] - self.dataset.variables['x'][0]
        #     self.dy = self.dataset.variables['y'][1] - self.dataset.variables['y'][0]
    def __geo_info__(self, msg):
        x, y = Proj(msg.projparams)(*msg.latlons()[::-1])
        nx = x.shape[1]
        ny = y.shape[0]
        x0 = x[0,0]
        y0 = y[0,0]
        dx = x[0,1] - self.x0
        dy = y[1,0] - self.y0
        return nx, ny, x0, y0, dx, dy
    # def __init_database__(self, msg):
    def __init_database__(self):
        # self.__geo_info__(msg)
        compression_args = {}

        self.dataset.createDimension('reference_time', 1)
        self.dataset.createDimension('time_since_reference', len(self.forecast_hours))    
        self.dataset.createDimension('y', self.ny)
        self.dataset.createDimension('x', self.nx)
        
        if self.compress:
            compression_args = {
                'zlib': True,
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
        cycle.units = 'hours since 2020-05-18 00:00:00.0'
        cycle.calendar = 'gregorian'

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
        
    def close(self):
        self.dataset.close()
    def append(self, filename, output_function=print):
        # output_function('{0:>11s}: {1:s}'.format('Reading', filename))
        grb = open_grib(filename)
        for i in range(grb.messages):
            msg = grb.message(i + 1)
            if matches_fields(msg, self.fields):
                if 'x' not in self.dataset.variables:
                    self.__init_database__(msg)
                field_info = get_variable_info(msg, self.fields)
                output_function('{0:>11s}: {1:s} - {2:s}'.format('Adding', filename, field_info['variable_name']))
                cycle_index = 0
                if is_masked(self.dataset.variables['reference_time'][cycle_index]):
                    cycle = date2num(msg.analDate, units=self.dataset.variables['reference_time'].units, calendar=self.dataset.variables['reference_time'].calendar)
                    self.dataset.variables['reference_time'][cycle_index] = cycle
                anal_date = num2pydate(self.dataset.variables['reference_time'][cycle_index], units=self.dataset.variables['reference_time'].units, calendar=self.dataset.variables['reference_time'].calendar)
                forecast_hour = int((msg.validDate - anal_date).total_seconds()/3600)
                forecast_hour_index = ([y for x in where(self.forecast_hours==forecast_hour) for y in x] + [None])[0]
                field_var = self.dataset.variables[field_info['variable_name']]
                nx,ny, x0, y0, _, _ = self.__geo_info__(msg)
                x_offset = int(round((self.x0-x0)/self.dx))
                y_offset = int(round((self.y0-y0)/self.dy))
                if cycle_index is not None and forecast_hour_index is not None:
                    field_var[cycle_index,forecast_hour_index,:,:] = msg.values[y_offset:self.ny+y_offset, x_offset:self.nx+x_offset]
        grb.close()

def process_directory(path, on_each, on_final, filter_func=None):
    raw_files = sorted([join(path, 'incoming', x) for x in listdir(join(path, 'incoming')) if x not in ['.DS_Store']])
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
    raw_files = sorted([join(path, 'incoming', x) for x in listdir(join(path, 'incoming')) if x not in ['.DS_Store']])
    if filter_func is None:
        raw_files = [[x] for x in raw_files]
    else:
        raw_files = filter_func(raw_files)
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

# if __name__ == '__main__':
    # from os.path import splitext
    # def group(files):
    #     output = {}
    #     for x in files:
    #         key = splitext(split(x)[1])[0].split('_')[0]
    #         if key not in output:
    #             output[key] = []
    #         output[key].append(x)
    #     return [output[k] for k in sorted(output.keys())]
    # def on_init():
    #     return (None, )
    # def on_each(filename, storage):
    #     data = 
    # process_directory('data/metars', None, None, None, filter_func=group)
    # # import pygrib as pg
    # # grb = pg.open('data/rtma/incoming/RTMA_2021021800.grib2')
    # # print(get_variable_info(grb.message(1), rtma_fields))
    # # grb.close()