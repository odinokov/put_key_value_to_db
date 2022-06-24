# put_key_value_to_db
read stdout as key and value pairs and store them as RocksDB

---
# Usage:

For tab separated stdout, where key-value pairs are separated by tab:

`echo -e 'key\tvalue' | python3 put_key_value_to_db.py ./dummy_db $'\t'`

## To read key do
```python
rocksdb_fm_path = './dummy_db'
key = 'key'
db = None # workaround to prevent LOCK issue
db = open_rocksdb(rocksdb_fm_path)
value = db.get(key.encode()).decode('utf-8')
print(value)
```
