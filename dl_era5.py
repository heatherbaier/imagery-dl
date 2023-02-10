from mpi4py import MPI

import geopandas as gpd
import calendar
import zipfile
import pygee
import os

from utils import *


def chunker_list(seq, size):
    return (seq[i::size] for i in range(size))


if __name__ == "__main__":

    BASE_DIR = "/sciclone/geograd/heather_data/era5/"

    # Set up the MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()    

    print(rank, size)

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
    
    gdf = pygee.convert_to_bbox(shp)
    gdf = gdf.rename(columns = {"GEOLEVEL2": "shapeID"})

    with open("/sciclone/home20/hmbaier/temporal_mex/log.txt", "a") as f:
        f.write("Number of polys in shp (no bounds): " + str(len(shp)))

    with open("/sciclone/home20/hmbaier/temporal_mex/log.txt", "a") as f:
        f.write("Number of polys in shp (bounds): " + str(len(gdf)))        

    IC = "ECMWF/ERA5/MONTHLY"
    BANDS = ["mean_2m_air_temperature", "minimum_2m_air_temperature", "maximum_2m_air_temperature", "total_precipitation"]
    RANK_DIREC = os.path.join(BASE_DIR, all_dates[0], all_dates[1])

    i = 0
    for col, row in gdf.iterrows():
        
        dates = pygee.GetDays(all_dates[0], all_dates[1])

        if str(row.shapeID) not in os.listdir(RANK_DIREC):

            try:

                pygee.download_imagery(geom = row.geometry,
                                    shapeID = row.shapeID,
                                    ic = IC, 
                                    dates = dates, 
                                    imagery_dir = RANK_DIREC, 
                                    bands = BANDS,
                                    scale = 1000)

                zipped = os.path.join(RANK_DIREC, row.shapeID) + "/" + os.listdir(os.path.join(RANK_DIREC, row.shapeID))[0]
                                    
                with zipfile.ZipFile(zipped, 'r') as zip_ref:
                    zip_ref.extractall(os.path.join(BASE_DIR, "extracted"))

                # pygee.save_pngs(shapeID = row.shapeID,
                #                 base_dir = RANK_DIREC,
                #                 export_dir = 
                #                 l = 5)

            except Exception as e:

                print(e)

                i += 1
