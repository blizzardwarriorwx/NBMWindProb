from multiprocessing import RLock
import queue
from pygrib import open as open_grib
from queue import Empty
from util import matches_fields, get_variable_info

def verify_process(queue, lock, fields, optional_variables):
    output = open(lock, 'w')
    done = False
    while not done:
        try:
            filename = queue.get(timeout=10)
            required_variables = [x['variable_name'] for x in fields]
            found_variables = []
            return_code = []

            grb = open_grib(filename)
            for i in range(grb.messages):
                msg = grb.message(i+1)
                if matches_fields(msg, fields):
                    info = get_variable_info(msg, fields)
                    if info['variable_name'] in required_variables:
                        found_variables.append(required_variables.pop(required_variables.index(info['variable_name'])))
                    elif info['variable_name'] in found_variables:
                        return_code.append('Duplicate field')
                    else:
                        return_code.append('Extra field')
            grb.close()
            if len(required_variables) > 0:
                i = 0
                while i < len(required_variables):
                    if required_variables[i] in optional_variables:
                        required_variables.pop(i)
                    else:
                        i += 1
                if len(required_variables) > 0:
                    return_code.append('Missing fields')
            if len(return_code) == 0:
                return_code = 'File good'
            else:
                return_code = ' '.join(set(return_code))
            # lock.acquire()
            # try:
            output.write('{0:s}: {1:s}\n'.format(filename, return_code))
            output.flush()
            # finally:
            #     lock.release()
        except Empty:
            output.close()
            done = True
    return 0

if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv
    from os import listdir, remove
    from os.path import join
    from multiprocessing import Process, Queue, Lock
    from util import nbm_fields, rtma_fields

    parser = ArgumentParser('verify_nbm.py', description="Verify the integrity of downloaded NBM grib files")
    parser.add_argument('directory', type=str, help='Directory containing data to verify')
    parser.add_argument('-r', '--rtma', dest='dataset', action='store_const', const="rtma", help="Verify RTMA data")
    parser.add_argument('-n', '--nbm', dest='dataset', action='store_const', const="nbm", help="Verify NBM data")
    parser.add_argument('-p', '--processes', metavar='#', dest='processes', type=int, default=1, help='Number of sub-processes to use(default=1)')
    opts = parser.parse_args(argv[1:])
    
    fields = None
    optional = []

    if opts.dataset is None:
        print('Must chose a dataset, -r/--rtma or -n/--nbm\n')
        parser.parse_args(['-h'])
    elif opts.dataset == 'rtma':
        fields = rtma_fields
    elif opts.dataset == 'nbm':
        fields = nbm_fields
        optional = ['2_meter_maximum_air_temperature_mean',
                    '2_meter_maximum_air_temperature_standard_deviation',
                    '2_meter_minimum_air_temperature_mean',
                    '2_meter_minimum_air_temperature_standard_deviation']
    
    if fields is not None:
        queue = Queue()
        # lock = RLock()
        [queue.put(join(opts.directory, x)) for x in listdir(opts.directory) if x[0] != '.']
        processes = [Process(target=verify_process, args=(queue, '{0:s}_verify_{1:d}.txt'.format(opts.dataset, i), fields, optional)) for i in range(opts.processes)]
        [x.start() for x in processes]
        processes[0].join()
        with open('{0:s}_verify.txt'.format(opts.dataset), 'w') as out_file:
            for i in range(opts.processes):
                with open('{0:s}_verify_{1:d}.txt'.format(opts.dataset, i), 'r') as in_file:
                    out_file.write(in_file.read())
                remove('{0:s}_verify_{1:d}.txt'.format(opts.dataset, i))