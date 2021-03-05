from os import listdir
from os.path import join

def listfiles():
    dates = {}
    data_path = join('data', 'rtma')
    for sub_folder in ['incoming', 'processed']:
        full_path = join(data_path, sub_folder)
        for filename in listdir(full_path):
            if len(filename) > 4 and filename[:4] == 'RTMA':
                if filename[5:13] not in dates:
                    dates[filename[5:13]] = []
                dates[filename[5:13]].append(filename[13:15])
        
    return ['{0:4s}-{1:2s}-{2:2s} [{3:s}]'.format(date[:4], date[4:6], date[6:], ' '.join(sorted(dates[date]))) for date in sorted(dates.keys())]

if __name__ == '__main__':
    print('\n'.join(listfiles()))
