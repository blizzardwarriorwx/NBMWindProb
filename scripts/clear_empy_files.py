from os import listdir, remove
from os.path import join, getsize

if __name__ == '__main__':
    data_dir = join('data', 'rtma', 'incoming')
    for filename in [join(data_dir, x) for x in sorted(listdir(data_dir)) if getsize(join(data_dir, x)) == 0]:
        print('Removing {0:s}'.format(filename))
        remove(filename)