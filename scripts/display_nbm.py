import metpy
import xarray as xr
import matplotlib.pyplot as plt
from cartopy.crs import PlateCarree
from cartopy.io.shapereader import Reader
from cartopy.feature import ShapelyFeature

def display(filename, field='elevation', hour=0):
    data = xr.open_dataset(filename)
    tmp = data.metpy.parse_cf(field)
    crs = tmp.metpy.cartopy_crs
    grid_bounds = [tmp.x.values.min(), tmp.x.values.max(), tmp.y.values.min(), tmp.y.values.max()]
    ax = plt.axes(projection=crs)
    ax.set_extent(grid_bounds, crs=crs)
    shape_feature = ShapelyFeature(Reader('data/cb_2019_us_county_500k/cb_2019_us_county_500k.shp').geometries(), PlateCarree(), edgecolor='black') # cb_2019_us_county_500k/cb_2019_us_county_500k.shp
    ax.add_feature(shape_feature, facecolor="None")
    ax.pcolormesh(tmp.x, tmp.y, tmp.sel(time_since_reference=hour).isel(reference_time=0).data, transform=crs)
    pts = xr.open_dataset('data/observations/locations.nc').to_dataframe()
    ax.plot(pts.Longitude, pts.Latitude, 'o', transform=PlateCarree())
    [ax.text(pts.Longitude[i], pts.Latitude[i], pts.StationName[i], transform=PlateCarree()) for i in pts.index]
    plt.show()

if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv

    parser = ArgumentParser('display_rtma.py', description="Display NBM Data")
    parser.add_argument('file', metavar="FILENAME", type=str, help="File to display")
    parser.add_argument('-f', '--field', metavar='FIELD', dest='field', type=str, default=None, help='Field to display')
    parser.add_argument('-t', '--hour', metavar='HOUR', dest='hour', type=int, default=0, help='Hour to display')
    opts = parser.parse_args(argv[1:])

    display(opts.file, field=opts.field, hour=opts.hour)
    