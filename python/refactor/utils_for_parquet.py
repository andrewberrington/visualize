'''
   tools for refactoring/working with parquet files (more details to come later)
'''

import pyarrow.parquet as pq
from collections import defaultdict
import numpy as np

def process_pq(pq_list, the_type):
    '''
    function to process a list of pq files and return appropriate
    3D coordinates

    Parameters
    ----------

    pq_list: list of parquet files
        the list of parquet files for processing

    the_type: string
        cloud tracking type (e.g. condensed, core, plume)

    Returns
    -------

    full: dictionary
        dictionary containing all of the coordinates from the pq files
        for each timestep

    sub: dictionary
        dictionary containing the coordinates of the inputted tracking type
        and also the 3D minima and maxima of these coordinates
        for each timestep

    c_id: float
        the LES id number of the cloud taken from the parquet files

    extrema: dictionary
        dictionary containing the 3D absolute maximum and minimum coordinates
        from the parquet files for each timestep
    '''

    keys = {
        "condensed": 0,
        "condensed_edge": 1,
        "condensed_env": 2,
        "condensed_shell": 3,
        "core": 4,
        "core_edge": 5,
        "core_env": 6,
        "core_shell": 7,
        "plume": 8,
        }

    # to get both the full domain of x and y along with the x and y coordinates
    # for the given type (condensed, core, etc.)

    # establish dictionaries to contain the various subsets of coordinates
    full = defaultdict(list)
    sub = defaultdict(list)
    extrema = defaultdict(list)

    # go through the list of parquet files and perform operations to populate
    # the dictionaries
    for f in pq_list:
        table = pq.read_table(f).to_pandas()
        c_id = table['cloud_id'].values[0]
        if the_type == 'full' or the_type == 'base':
            tablerows = table['type'] == keys["condensed"]
        else:
            tablerows = table['type'] == keys[the_type]
        df_thetype = table[tablerows]
        for dimension in ['x', 'y', 'z']:
            for suffix in ['full', 'sub']:
                if suffix == 'full':
                    full[(dimension, suffix)].append(table[dimension].values)
                else:
                    sub[(dimension, suffix)].append(df_thetype[dimension].values)
            for suffix in ['min', 'max']:
                if suffix == 'min':
                    extrema[(dimension, suffix)].append(np.amin(table[dimension].values))
                    sub[(dimension, suffix)].append(np.amin(df_thetype[dimension].values))
                else:
                    extrema[(dimension, suffix)].append(np.amax(table[dimension].values))
                    sub[(dimension, suffix)].append(np.amax(table[dimension].values))
    return full, sub, c_id, extrema