from mpi4py import MPI

import geopandas as gpd
import calendar
import pygee
import json
import os

from utils import *


def chunker_list(seq, size):
    return (seq[i::size] for i in range(size))


if __name__ == "__main__":

    BASE_DIR = "/sciclone/geograd/heather_data/missing_0419_v2/"

    # Set up the MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()    

    print(rank, size)

    with open("/sciclone/geograd/Heather/temporal_mex/data/missing_map_0419_v2.json", "r") as f:
        missing_munis = json.load(f)

    if rank == 0:

        num_per_rank = int( len(missing_munis.keys()) / size)
        
        all_dates = list(missing_munis.keys())#[0:100]

        # n = 1

        all_dates = [all_dates[i:i + num_per_rank] for i in range(0, len(all_dates), num_per_rank)][0:size]

        print(all_dates)

    else:

        all_dates = None

    all_dates = comm.scatter(all_dates, root = 0)

    print("RANK: ", rank, "  ALL DATES: ", all_dates)

    for date in all_dates:

        shp = gpd.read_file("/sciclone/geograd/Heather/temporal_mex/data/geo2_mx1960_2015.shp")

        print("Rank: ", rank, date, missing_munis[date])

        muni = date
        date = missing_munis[date]
        month, year = date.split("_")[1], date.split("_")[0]

        IC = "LANDSAT/LT05/C02/T1_L2"
        BANDS = ['SR_B3', 'SR_B2', 'SR_B1']

        dates = pygee.GetDays(year, month)

        shp = shp[shp["GEOLEVEL2"] == muni]

        print(shp)

        pygee.download_imagery(geom = shp["geometry"].to_list()[0],
                                shapeID = shp["GEOLEVEL2"].to_list()[0],
                                ic = IC, 
                                dates = dates, 
                                imagery_dir = BASE_DIR, 
                                bands = BANDS)

        pygee.save_pngs(shapeID = shp["GEOLEVEL2"].to_list()[0],
                        base_dir = BASE_DIR,
                        export_dir = os.path.join(BASE_DIR, "extracted"),
                        l = 5)


