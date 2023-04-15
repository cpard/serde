# serde

## dependencies
the project only requires duckdb at this point, polars is added there for later experiments.
I'm using [pdm](https://pdm.fming.dev/latest/) for dependency and project management together with venv for 
virtual environment management. all the tests have been done so far using python 3.11.

## getting the data
The current implementation expects a "../data/" folder (assuming you execute at the root of the project dir)
to get the data run wget `https://data.gharchive.org/2023-01-03-{0..23}.json.gz` this will get the daily data for
2023-01-03. If you want more data you can do something like: `wget https://data.gharchive.org/2023-{0..12}-{0..31}-{0..23}.json.gz` this should get you all the data for 2023 (be careful as each day is ~2G compressed).

## running the examples
The implementation is not complete yet but you can do the following:
`duck_serde.py` will read the compressed json files and generate one big parquet file inside `../data/parquet/`
it will also print execution times.

`stg_all_actions_for_actors_who_starred_repo.py` builds the first staging table based on the fake star detector.
The output will be stored in `../data/parquet/` as a parquet file. It will also execute the query both in the raw
JSON files and on the parquet file and print times, it will also write out a parquet file for the staging table
using the parquet input. 

**TODO** I have to finish the rest of the pipeline stages + finish the Rust baseline implementation (rusty_serde)

 
