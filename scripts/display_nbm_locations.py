from xarray import open_mfdataset, open_dataset
from os.path import join, split
from util import latlon2ij, xy2latlon, distance
from pandas import concat

def process(file):
    dataset = open_dataset(file)
    locations = open_dataset(join('data', 'observations', 'locations.nc')).to_dataframe()
    output = []
    for loc_id in locations.Site.unique():
        location_info = locations[locations.Site == loc_id].reset_index(drop=True)
        i, j = latlon2ij(dataset, location_info.Latitude[0], location_info.Longitude[0])
        extracted = dataset.isel(x=i, y=j).to_dataframe()
        location_info['NBM_X'] = extracted.x.unique()[0]
        location_info['NBM_Y'] = extracted.y.unique()[0]
        location_info[['NBM_Lat', 'NBM_Lon']] = xy2latlon(dataset, location_info.NBM_X[0], location_info.NBM_Y[0])
        location_info['Distance'] = distance(location_info.NBM_Lon[0], location_info.NBM_Lat[0], location_info.Longitude[0], location_info.Latitude[0])
        output.append(location_info)
    
    output = concat(output).reset_index(drop=True)

    output.to_csv(join('data', 'observations', 'nbm_locations.csv'), index=False)
if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv

    parser = ArgumentParser('display_nbm_locations.py', description="Extract the postion of locations in the NBM grid")
    parser.add_argument('file', metavar="FILE", type=str, help="NBM file to use.")
    
    opts = parser.parse_args(argv[1:])
    process(opts.file)
