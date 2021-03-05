from os import listdir
from os.path import join, split
from xarray import open_dataset
from pyproj import Proj
from numpy import linspace, min, max, floor, ceil, arange

def get_bounds(lower, left, upper, right, dataset):
    proj = Proj(dataset.lambert_conformal_conic.proj4params)
    lat = (list(linspace(lower, upper, 5)[:-1]) + 
           list(linspace(upper, upper, 5)[:-1]) + 
           list(linspace(upper, lower, 5)[:-1]) + 
           list(linspace(lower, lower, 5)[:-1]))
    lon = (list(linspace(left,  left,  5)[:-1]) + 
           list(linspace(left,  right, 5)[:-1]) + 
           list(linspace(right, right, 5)[:-1]) + 
           list(linspace(right, left,  5)[:-1]))
    x, y = proj(lon, lat)
    x0 = dataset.x.values[0]
    y0 = dataset.y.values[0]
    dx = dataset.x.values[1] - x0
    dy = dataset.y.values[1] - y0
    x1 = int(floor((min(x) - x0) / dx))
    x2 = int(ceil((max(x) - x0) / dx))
    y1 = int(floor((min(y) - y0) / dy))
    y2 = int(ceil((max(y) - y0) / dy))
    return x1, x2, y1, y2

if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv
    "--left=-113.062055 --lower=43.639387 --right=-103.197756 --upper=47.589510 --name=BYZ"
    parser = ArgumentParser('subset_data.py', description="Subset NetCDF files")
    parser.add_argument('files', metavar="FILE", type=str, nargs="+", help="Files to subset.")
    parser.add_argument('--left', metavar='LON', type=float, required=True, help='Left bound')
    parser.add_argument('--right', metavar='LON', type=float, required=True, help='Right bound')
    parser.add_argument('--lower', metavar='LAT', type=float, required=True, help='Lower bound')
    parser.add_argument('--upper', metavar='LAT', type=float, required=True, help='Upper bound')
    parser.add_argument('--name', metavar='NAME', type=str, required=True, help='Name for subset')
    opts = parser.parse_args(argv[1:])
    
    for filename in opts.files:
        dir_part, file_part = split(filename)
        file_part = file_part.replace('.nc', '_{0:s}.nc'.format(opts.name))
        print(filename)
        ds = open_dataset(filename, backend_kwargs=dict(mode='r'))
        x1, x2, y1, y2 = get_bounds(opts.lower, opts.left, opts.upper, opts.right, ds)
        ds.isel(x=arange(x1, x2+1),  y=arange(y1, y2+1)).to_netcdf(join(dir_part, file_part))
        ds.close()
        