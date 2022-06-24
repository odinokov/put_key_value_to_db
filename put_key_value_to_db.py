# read stdout as key and value pairs and store them as RocksDB

import sys, argparse, rocksdb
# import gc

def get_args() -> str:
    """Read arguments from command line

    Returns:
        str: the name of RocksDB database, key and value field separator 
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('rocksdb_fm_path', type=str, help='- path to RocksDB database')
    parser.add_argument('key_value_separator', type=str, help='- key and value separator')

    args = parser.parse_args()

    return args.rocksdb_fm_path, args.key_value_separator

def read_stdin() -> str:
    for line in sys.stdin:
        yield line.rstrip()

def open_rocksdb(rocksdb_fm_path: str):
    """create a RocksDB object

    Args:
        rocksdb_fm_path (str): RocksDB name

    Returns:
        _type_: RocksDB object
    """
    
    options = rocksdb.Options()
    options.create_if_missing = True
    options.max_open_files = 10  # ubuntu 16.04 max files descriptors 524288
    options.write_buffer_size = 128 * 1024 * 1024  # 128 MiB
    options.max_write_buffer_number = 3
    options.target_file_size_base = 67108864
    options.compression = rocksdb.CompressionType.lz4_compression
    
    # db = None 
    
    db = rocksdb.DB(rocksdb_fm_path, options, read_only=False)

    return db

# main

rocksdb_fm_path, key_values_separator = get_args()

db = open_rocksdb(rocksdb_fm_path)

read_stdin_generator = read_stdin()       

line = next(read_stdin_generator, False)

while line:

    # get tab separated string like this:
    # key	value

    key, value = line.split(key_values_separator)

    db.put(key.encode(), value.encode())

    line = next(read_stdin_generator, False)

# del db; gc.collect()

# to read key do
# key = 'key'
# db = None 
# db = open_rocksdb(rocksdb_fm_path)
# print(db.get(key.encode()).decode('utf-8'))

sys.exit(0)
