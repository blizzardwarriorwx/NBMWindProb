from re import search, sub
from gzip import open as gzip_open
from sqlite3 import connect
from os.path import join
from shutil import move

data_dir = 'data/database'

def import_database(db, data):
    cursor = db.cursor()
    index = 0
    match = search('CREATE TABLE[^;]+;', data[index:])
    while match is not None:
        sql_cmd = sub('\\) ENGINE[^;]+;', ');', match.group(0)).replace('CHARACTER SET hp8', '')
        print(sql_cmd.replace('\n', ' '))
        cursor.execute(sql_cmd)
        db.commit()
        index += match.span()[1]
        match = search('CREATE TABLE[^;]+;', data[index:])
    index = 0
    match = search('INSERT INTO[^;]+;', data[index:])
    while match is not None:
        sql_cmd = match.group(0)
        print(sql_cmd.replace('\n', ' '))
        cursor.execute(sql_cmd)
        db.commit()
        index += match.span()[1]
        match = search('INSERT INTO[^;]+;', data[index:])
    cursor.close()




if __name__ == '__main__':
    from argparse import ArgumentParser
    from sys import argv

    parser = ArgumentParser('mysql2sqlite.py', description="Convert MySQL database to SQLite")
    parser.add_argument('database', metavar="DB", type=str, help="Database to read")
    parser.add_argument('file', metavar="FILE", type=str, help="File to import")
    parser.add_argument('-z', '--gzip', dest='gzip', action='store_true', help='Use gzip to open file')
    opts = parser.parse_args(argv[1:])
    
    db = connect(join(data_dir, 'incoming', opts.database))
    if opts.gzip:
        with gzip_open(join(data_dir, 'incoming', opts.file), 'rb') as in_file:
            data = in_file.read().decode('utf-8')
    else:
        with open(join(data_dir, 'incoming', opts.file), 'r') as in_file:
            data = in_file.read()
    import_database(db, data)
    move(join(data_dir, 'incoming', opts.file), join(data_dir, 'processed', opts.file))
    db.commit()
    db.close()