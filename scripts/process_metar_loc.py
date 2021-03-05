"""
Process METAR Locations in GEMPAK format from Iowa State - Iowa Enviromental Mesonet (https://mesonet.agron.iastate.edu/sites/networks.php)
"""
from pandas import DataFrame, concat
from os.path import join, exists
from xarray import open_dataset

def process(filename):
    output_file = join('data', 'observation_loc.nc')
    fields = [0, 6, 15, 48, 51, 54, 60, 67, 73]

    with open(filename, 'r') as in_file:
        data = DataFrame([dict([(['Site', 'WBAN', 'StationName','State', 'Country', 'Latitude', 'Longitude', 'Elevation'][i], [str, lambda a: None if a == '' else int(a), str, str, str, lambda a:float(a)/100.0, lambda a: float(a)/100, float][i](x[y[0]:y[1]].strip())) for i, y in enumerate(zip(fields[:-1], fields[1:]))]) for x in in_file.read().split('\n')])

    data = data[['Site', 'StationName','State', 'Country', 'Latitude', 'Longitude', 'Elevation']]
    if exists(output_file):
        data = concat(data, open_dataset(output_file).to_dataframe())
    
    data.to_xarray().to_netcdf(output_file)
    

if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv

    parser = ArgumentParser('process_metar_loc.py', description="Convert METAR locations in GEMPACK to NetCDF4 file.")
    parser.add_argument('filename', metavar="file", type=str, help="Filename to convert")
    
    opts = parser.parse_args(argv[1:])

    process(opts.filename)