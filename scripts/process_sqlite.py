from sqlalchemy import create_engine
from sqlalchemy.sql import text
from pandas import read_sql_table, read_sql_query
from os.path import join

data_dir = 'data/database'

def extract(filename, filter_tables=[]):
    filename = join(data_dir, 'incoming', filename)
    engine = create_engine("sqlite:///{0:s}".format(filename))
    with engine.connect() as conn, conn.begin():
        conn.execute(text('UPDATE `UMHMwind` SET `dt` = "0001-01-01 00:00:00" WHERE `dt` = "0000-00-00 00:00:00"'))
        conn.execute(text('UPDATE `MOSDATA` SET `run` = "0001-01-01 00:00:00" WHERE `run` = "0000-00-00 00:00:00"'))
        conn.execute(text('UPDATE `MOSDATA` SET `valid` = "0001-01-01 00:00:00" WHERE `valid` = "0000-00-00 00:00:00"'))
        tables = read_sql_query("SELECT name FROM sqlite_master WHERE type IN ('table','view')  AND name NOT LIKE 'sqlite_%' ORDER BY 1;", conn)
        tables = tables[tables.name.apply(lambda a: a in filter_tables if len(filter_tables) > 0 else True)]
        for tablename in tables.name:
            data_frame = read_sql_table(tablename, conn)
            data_frame.columns = [a.replace('/', '_') for a in data_frame.columns]
            data_frame.to_xarray().to_netcdf(join(data_dir, '{0:s}.nc'.format(tablename)), encoding=dict([(column, {'zlib': True, 'complevel': 9, 'shuffle': True}) for column in data_frame.columns]))


if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv

    parser = ArgumentParser('subset_data.py', description="Convert SQLite database to NetCDF files")
    parser.add_argument('database', metavar="DB", type=str, help="Database to read")
    parser.add_argument('--tables', metavar="TABLES", type=str, default='', help="Comma separated list of tables to extract (Optional)")
    opts = parser.parse_args(argv[1:])
    opts.tables = [x.strip() for x in opts.tables.split(',') if x != '']
    extract(opts.database, filter_tables=opts.tables)