import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import pint

units = pint.UnitRegistry()
mpsKnot = units('m s**-1').to(units('knot')).magnitude

mps   = xr.open_dataset('data/metars/6S0.nc').to_dataframe()

mps['10_meter_wind_speed_of_gust'] = round(mps['10_meter_wind_speed_of_gust'] * mpsKnot)
mps['10_meter_wind_speed'] = round(mps['10_meter_wind_speed'] * mpsKnot)



# mps.groupby(['10_meter_wind_speed']).boxplot(subplots=False, column='10_meter_wind_speed_of_gust', rot=90)
mps.boxplot(column=['10_meter_wind_speed_of_gust'], by=['10_meter_wind_speed'], rot=45)
plt.show()