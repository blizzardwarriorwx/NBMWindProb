from pandas.core.tools.datetimes import to_datetime
from pint.unit import Unit
from xarray import open_dataset
from numpy import isnan, isfinite, array, round
from pandas import to_timedelta, merge
from pint import UnitRegistry

units = UnitRegistry()

def link_observations(obs_file, rtma_file):
    big_timber_obs = open_dataset(obs_file).to_dataframe()
    for field in ['10_meter_wind_speed', '10_meter_wind_speed_of_gust']:
        big_timber_obs[field] = (array(big_timber_obs[field]) * units('m s**-1')).to(units('knots')).magnitude
    big_timber_obs = big_timber_obs[isfinite(big_timber_obs['10_meter_wind_speed'])]
    big_timber_obs.time = big_timber_obs.time.dt.round('H')
    # big_timber_obs.loc[isnan(big_timber_obs['10_meter_wind_speed_of_gust']), '10_meter_wind_speed_of_gust'] = big_timber_obs[isnan(big_timber_obs['10_meter_wind_speed_of_gust'])]['10_meter_wind_speed']
    big_timber_obs = big_timber_obs.groupby(['time']).max().reset_index()
    big_timber_obs.columns = [x + '_obs' for x in big_timber_obs.columns]

    big_timber_rtma = open_dataset(rtma_file).to_dataframe()
    for field in ['10_meter_wind_speed', '10_meter_wind_speed_of_gust']:
        big_timber_rtma[field] = (array(big_timber_rtma[field]) * units('m s**-1')).to(units('knots')).magnitude
    big_timber_rtma['time'] = [big_timber_rtma.reference_time[i] + to_timedelta('{0:.0f}H'.format(big_timber_rtma.time_since_reference[i])) for i in big_timber_rtma.index]
    big_timber_rtma.columns = [x + '_rtma' for x in big_timber_rtma.columns]

    return merge(big_timber_obs, big_timber_rtma, how='inner', left_on='time_obs', right_on='time_rtma').reset_index()

if __name__ == '__main__':
    from matplotlib import pyplot as plt
    from scipy import stats
    from numpy import array
    data = link_observations('data/metars/6S0.nc', 'data/rtma/RTMA_6S0.nc')
    print(data)
    field = '10_meter_wind_speed'
    data[field + "_rtma_1"] = round(array(data[field + "_rtma"]) / 1.0) * 1.0
    data1 = data.groupby([field + "_rtma_1"]).mean().reset_index()
    y = '_obs'
    x = '_rtma'
    data = data[data[field+'_obs'] >= 0]
    regression = stats.linregress(data1[field + x], data1[field + y])
    print(regression)
    plt.plot(regression.slope * data[field + x] + regression.intercept, data[field + y], 'o')
    plt.plot((0,70), (0, 70))
    plt.plot(array([data[field+x].min(), data[field+y].max()]), regression.slope * array([data[field+x].min(), data[field+y].max()]) + regression.intercept)

    plt.show()

    # obs = plt.plot(data['time' + y], data[field + y], label='Obs')
    # rtma = plt.plot(data['time' + x], data[field + x], label='RTMA')
    # plt.legend()
    # plt.show()
