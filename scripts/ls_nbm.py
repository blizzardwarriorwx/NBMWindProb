from os import listdir
from os.path import join

def listfiles():
    dates = {}
    data_path = join('data', 'nbm')
    for sub_folder in ['incoming', 'processed']:
        full_path = join(data_path, sub_folder)
        for filename in listdir(full_path):
            if len(filename) > 3 and filename[:3] == 'nbm':
                if filename[10:18] + filename[19:21] not in dates:
                    dates[filename[10:18] + filename[19:21]] = []
                dates[filename[10:18] + filename[19:21]].append(str(int(filename[24:27])))
        
    return ['{0:4s}-{1:2s}-{2:2s} {3:2s}Z [{4:s}]'.format(date[:4], date[4:6], date[6:8], date[8:], ' '.join(sorted(dates[date]))) for date in sorted(dates.keys())]

if __name__ == '__main__':
    print('\n'.join(listfiles()))
