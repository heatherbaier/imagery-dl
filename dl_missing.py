from mpi4py import MPI

import geopandas as gpd
import calendar
import pygee
import os

from utils import *


def chunker_list(seq, size):
    return (seq[i::size] for i in range(size))


if __name__ == "__main__":

    BASE_DIR = "/sciclone/geograd/heather_data/missing_0224/"

    # Set up the MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()    

    print(rank, size)

    with open("/sciclone/geograd/Heather/temporal_mex/missing_imagery_0224.txt", "r") as f:
        missing = f.read().splitlines()

    if rank == 0:
        
        all_dates = []

        for year in range(1995, 2001):

            year_direc = os.path.join(BASE_DIR, str(year))
            if not os.path.isdir(year_direc):
                os.mkdir(year_direc)            

            for month in range(1, 13):

                month_direc = os.path.join(BASE_DIR, str(year), str(month))
                if not os.path.isdir(month_direc):
                    os.mkdir(month_direc)                

                all_dates.append([str(year), str(month)])

        print(all_dates)

    else:

        all_dates = None

    all_dates = comm.scatter(all_dates, root = 0)


    shp = gpd.read_file("/sciclone/geograd/Heather/temporal_mex/data/geo2_mx1960_2015.shp")
    shp = shp.dropna(subset = ['geometry'])
    shp = shp.set_crs("EPSG:4326")
    shp = shp.to_crs("EPSG:6362")
    shp = pygee.remove_islands(shp, "GEOLEVEL2")
    shp = shp.to_crs("EPSG:4326")
    shp = pygee.convert_to_bbox(shp)
    shp = shp.rename(columns = {"GEOLEVEL2": "shapeID"})
    shp = shp.sort_values(by = "shapeID")


    IC = "LANDSAT/LT05/C02/T1_L2"
    BANDS = ['SR_B3', 'SR_B2', 'SR_B1']
    RANK_DIREC = os.path.join(BASE_DIR, all_dates[0], all_dates[1])


    i = 0
    for col, row in shp.iterrows():
        
        dates = pygee.GetDays(all_dates[0], all_dates[1])

        if str(row.shapeID) not in os.listdir(RANK_DIREC):

            test = str(row.shapeID) + " " + str(all_dates[0]) + " " + str(all_dates[1])

            if test in missing:

                try:
        
                    pygee.download_imagery(geom = row.geometry,
                                            shapeID = row.shapeID,
                                            ic = IC, 
                                            dates = dates, 
                                            imagery_dir = RANK_DIREC, 
                                            bands = BANDS)

                except Exception as e:

                    print(e)

                i += 1



