from pathlib import Path
import time
import polars as pl

data_folder = Path("../data/")
filename = '2023-01-01-15.json'
json_file = data_folder / filename
output_file_name = 'output.parquet'

start_time = time.time()
l = pl.read_ndjson(json_file)
print(l)
print("--- %s seconds ---" % (time.time() - start_time))