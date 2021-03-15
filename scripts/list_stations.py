from xarray import open_dataset
from os.path import join

data = open_dataset(join('data', 'observation_loc.nc')).to_dataframe()
print('')
print('{0: <10s}{1: <40s}{2: >6s}{3: >8s}{4: >10s}{5: >11s}{6: >11s}'.format(*data.columns))
for i in data.index:
    print('{0: <10s}{1: <40s}{2: >6s}{3: >8s}{4:10.4f}{5:11.4f}{6:11.1f}'.format(*data.iloc[i].to_numpy()))
print('\n')