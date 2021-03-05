from xarray import open_mfdataset, open_dataset
from os.path import join, split
from util import latlon2ij

def process(loc_id, files):
    dir_part, file_part = split(files[0])
    dest_file = join(dir_part, '{0:s}_{1}.nc'.format(file_part.split('_')[0], loc_id))
    locations = open_dataset(join('data', 'observation_loc.nc')).to_dataframe()
    location_info = locations[locations.Site == loc_id].reset_index(drop=True)
    dataset = open_mfdataset(files)
    i, j = latlon2ij(dataset, location_info.Latitude[0], location_info.Longitude[0])
    
    extracted = dataset.isel(x=i, y=j).to_dataframe().reset_index()

    extracted[[x for x in extracted.columns if x not in ['x', 'y', 'lambert_conformal_conic']]].to_xarray().to_netcdf(dest_file)

if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv

    parser = ArgumentParser('extract_location.py', description="Extract site time series from gridded NetCDF4 files.")
    parser.add_argument('files', metavar="FILE", type=str, nargs="+", help="Files to extract from.")
    parser.add_argument('--site', metavar="SITE", type=str, required=True, help="Site ID of location to extact")
    
    opts = parser.parse_args(argv[1:])
    process(opts.site, opts.files)
