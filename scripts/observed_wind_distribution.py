from xarray import open_dataset
from os.path import join
from numpy import isfinite, array, arange, isnan, log10
from pint import UnitRegistry
from matplotlib import pyplot as plt
from scipy import stats

log10 = lambda a:a


units = UnitRegistry()

field = '10_meter_wind_speed_of_gust'

mpsKnots = units('m s**-1').to(units('knots')).magnitude
knotsMPS = 1 / units('m s**-1').to(units('mph')).magnitude

data = open_dataset(join('data', 'metars', '6S0_m.nc')).to_dataframe()
# data.loc[isnan(data[field]), field] = data[isnan(data[field])]['10_meter_wind_speed']
# data = data[data[field] > 0].reset_index(drop=True)
bins = int(round(data[field].max()-data[field].min()))
data[field] = log10(data[field])
data[field] = round(data[field] * mpsKnots)
bins = int(round((data[field].max()-data[field].min())*0.25))

if __name__ == '__main__':
    gust_distribution = {}
    for x in data.index:
        key = data[field][x]
        if isfinite(key):
            key = round(key)
            if key not in gust_distribution:
                gust_distribution[key] = 1
            else:
                gust_distribution[key] += 1
    gust_distribution = array([(k, gust_distribution[k]) for k in sorted(gust_distribution.keys())], dtype='float64')
    gust_distribution[:,1] = gust_distribution[:,1] / float(sum([x for x in gust_distribution[:,1] if isfinite(x)]))
    # plt.bar(gust_distribution[:,0], gust_distribution[:,1])
    
    skew = ((float(data[field].mean()) - float(data[field].mode())) / float(data[field].std()))
    print(skew, data[field].skew(), data[field].std(), stats.skew(data[field], bias=True, nan_policy='omit'))
    plt.hist(data[field], bins, density=True)
    x = arange(0, data[field].max()+0.1, 0.1)
    print(100*(1-stats.norm(loc=data[field].mean(), scale=data[field].std()).cdf(log10(58*knotsMPS))))
    print(100*(1-stats.skewnorm(data[field].skew(), loc=data[field].mean(), scale=data[field].std()).cdf(log10(58*knotsMPS))))
    plt.plot(x, stats.norm(loc=data[field].mean(), scale=data[field].std()).pdf(x))
    plt.plot(x, stats.skewnorm(data[field].skew(), loc=data[field].mean(), scale=data[field].std()).pdf(x))
    plt.show()