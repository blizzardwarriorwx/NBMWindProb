from os import listdir
from os.path import join
from pygrib import open as open_grib
from util import nbm_fields, matches_fields, get_variable_info

def verify_file(filename):
    required_variables = [x['variable_name'] for x in nbm_fields]
    optional_variables = ['2_meter_maximum_air_temperature_mean',
                          '2_meter_maximum_air_temperature_standard_deviation',
                          '2_meter_minimum_air_temperature_mean',
                          '2_meter_minimum_air_temperature_standard_deviation']
    found_variables = []
    return_code = []
    grb = open_grib(filename)
    for i in range(grb.messages):
        msg = grb.message(i+1)
        if matches_fields(msg, nbm_fields):
            info = get_variable_info(msg, nbm_fields)
            if info['variable_name'] in required_variables:
                found_variables.append(required_variables.pop(required_variables.index(info['variable_name'])))
            elif info['variable_name'] in found_variables:
                return_code.append('Duplicate field')
            else:
                return_code.append('Extra field')
    grb.close()
    if len(required_variables) > 0:
        i = 0
        while i < len(required_variables):
            if required_variables[i] in optional_variables:
                required_variables.pop(i)
            else:
                i += 1
        if len(required_variables) > 0:
            return_code.append('Missing fields')
    if len(return_code) == 0:
        return_code = 'File good'
    else:
        return_code = ' '.join(set(return_code))
    return return_code

def verify_directory(data_dir):
    for filename in [x for x in sorted(listdir(data_dir)) if x.find('DS_Store') == -1]:
        file_path = join(data_dir, filename)
        print('{0:s}: {1:s}'.format(file_path, verify_file(file_path)))

if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv

    parser = ArgumentParser('verify_nbm.py', description="Verify the integrity of downloaded NBM grib files")
    parser.add_argument('directory', type=str, help='Directory containing data to verify')
    opts = parser.parse_args(argv[1:])
    verify_directory(opts.directory)