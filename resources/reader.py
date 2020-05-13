import json
import os
import pandas as pd


def load_cache(read_func, path, **kwargs):
    abspath = os.path.abspath(path)
    CACHE_PATH = os.path.join("cache", "dataframes")
    os.makedirs(CACHE_PATH, exist_ok=True)

    key = json.dumps({
        "path": abspath,
        **kwargs
    })

    # Associates each message path with cache file
    CACHE_INDEX = os.path.join(CACHE_PATH, "index.json")

    if os.path.isfile(CACHE_INDEX):
        with open(CACHE_INDEX) as f:
            index = json.load(f)

        try:
            cache_file = index[key]
        except KeyError:  # File is not cached
            read_source = True
        else:  # Check if cache file exists
            read_source = not os.path.isfile(cache_file)
    else:
        read_source = True

        with open(CACHE_INDEX, 'w') as f:  # Initialize index.json
            json.dump({}, f)

    if read_source:
        # Read number of entries
        with open(CACHE_INDEX) as f:
            count = len(json.load(f))

        filename = f"{count}.pkl"
        cache_file = os.path.abspath(os.path.join(CACHE_PATH, filename))

        # Write cache file
        df = read_func(abspath, **kwargs)
        df.to_pickle(cache_file)

        # Update index.json
        with open(CACHE_INDEX) as f:
            index = json.load(f)

        index[key] = cache_file

        with open(CACHE_INDEX, 'w') as f:
            json.dump(index, f)

    else:  # Load cache
        df = pd.read_pickle(cache_file)

    return df


def load_cols(read_func, files, cols=[], **kwargs):
    dfs = []
    for i in files:
        next_df = load_cache(read_func, i, **kwargs)
        drop_cols = set(next_df.columns) - {"timestamp", *cols}
        next_df.drop(drop_cols, axis=1, inplace=True)
        dfs.append(next_df)

    return pd.concat(dfs, ignore_index=True)
