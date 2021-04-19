from xarray import open_mfdataset, open_dataset
from os.path import join, split
from util import latlon2ij

def process(loc_id, lat, lon, files):
    dir_part, file_part = split(files[0])
    dest_file = join(dir_part, '{0:s}_{1}.nc'.format(file_part.split('_')[0], loc_id))
    dataset = open_mfdataset(files)
    i, j = latlon2ij(dataset, lat, lon)
    
    extracted = dataset.isel(x=i, y=j).to_dataframe().reset_index()

    extracted[[x for x in extracted.columns if x not in ['x', 'y', 'lambert_conformal_conic']]].to_xarray().to_netcdf(dest_file)

if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv

    parser = ArgumentParser('extract_point.py', description="Extract point time series from gridded NetCDF4 files.")
    parser.add_argument('files', metavar="FILE", type=str, nargs="+", help="Files to extract from.")
    parser.add_argument('--site', metavar="SITE", type=str, required=True, help="Site ID of location to extact")
    parser.add_argument('--lat', metavar="LAT", type=str, required=True, help="Latitude of the point to extact")
    parser.add_argument('--lon', metavar="LON", type=str, required=True, help="Longitude of the point to extact")
    opts = parser.parse_args(argv[1:])
    process(opts.site, opts.lat, opts.lon, opts.files)
