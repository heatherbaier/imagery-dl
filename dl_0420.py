from mpi4py import MPI


import pandas as pd
import numpy as np
import os


import geopandas as gpd
import calendar
import pygee
import json
import os

from utils import *


if __name__ == "__main__":

    BASE_DIR = "/sciclone/geograd/heather_data/large_test/"

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

    print("RANK: ", rank, "  ALL DATES: ", all_dates)

    with open("/sciclone/geograd/Heather/temporal_mex/current.txt", "r") as f:
        done = f.read()#
        
    done = done.replace("\n", " ").split(" ")
    done = [_.strip(".nc") for _ in done if _ != ""]
    # done

    ip = os.listdir("/sciclone/geograd/heather_data/netCDFs_0419_v1/") + os.listdir("/sciclone/geograd/heather_data/netCDFs_0419_v2/")
    ip = [_.strip(".nc") for _ in ip if ".ipynb" not in _]

    done = list(np.unique(done + ip))
    done = [int(i) for i in done]


    imagery = pd.read_csv("/sciclone/geograd/Heather/temporal_mex/imagery_as_of_0420.csv")
    imagery = imagery.set_index("GEOLEVEL2")
    imagery.head()


    import geopandas as gpd

    gdf = gpd.read_file("/sciclone/geograd/Heather/temporal_mex/data/geo2_mx1960_2015.shp")
    gdf = gdf.dropna(subset = ['geometry'])
    gdf = gdf.set_crs("EPSG:4326")
    gdf = gdf.to_crs("EPSG:6362")
    gdf["centroid"] = gdf.geometry.centroid
    gdf = gdf[~gdf['GEOLEVEL2'].astype(int).isin(done)]
    gdf.geometry = gdf["centroid"]
    gdf.geometry = gdf.buffer(60000, cap_style = 3)
    # shp = pygee.remove_islands(shp, "GEOLEVEL2")
    gdf = gdf.to_crs("EPSG:4326")
    gdf.head()


    import pygee

    IC = "LANDSAT/LT05/C02/T1_L2"
    BANDS = ['SR_B3', 'SR_B2', 'SR_B1']


    munis = gdf["GEOLEVEL2"].to_list()

    # for date in all_dates:

    for muni in munis:

        print(muni, all_dates)
        
        dates = pygee.GetDays(all_dates[0], all_dates[1])

        shp = gdf[gdf["GEOLEVEL2"] == str(muni)]

        direc = os.path.join(BASE_DIR, all_dates[0], all_dates[1])

        pygee.download_imagery(geom = shp["geometry"].to_list()[0],
                                shapeID = str(muni),
                                ic = IC, 
                                dates = dates, 
                                imagery_dir = direc,
                                bands = BANDS)
            
            
            
# import rasterio as rio
# from PIL import Image
# import numpy as np
# import zipfile
# import imageio
# import shutil
# import os



# def open_tifs(shapeID, base_dir, export_dir = None, v = True, l = 8):
    
#     FULL_DIR = os.path.join(base_dir)
#     TEMP_DIR = os.path.join(base_dir, "temp")

#     os.mkdir(TEMP_DIR)
    
#     dir_length = len(os.listdir(FULL_DIR))
#     num = 0

#     for zipfolder in os.listdir(FULL_DIR):
                
#         if v:
#             print("Image ", str(num), " of ", str(dir_length), "---- Month: May")

#         num += 1

#         image_name = zipfolder.split(".zip")[0]

#         # Extract the RGB TIFF files into the temporary directory
#         with zipfile.ZipFile(os.path.join(FULL_DIR, zipfolder), 'r') as zip_ref:
#             zip_ref.extractall(TEMP_DIR)

#         tiff_files = os.listdir(TEMP_DIR)

#         b1 = os.path.join(TEMP_DIR, [i for i in tiff_files if i.endswith("B1.tif")][0])
#         b2 = os.path.join(TEMP_DIR, [i for i in tiff_files if i.endswith("B2.tif")][0])
#         b3 = os.path.join(TEMP_DIR, [i for i in tiff_files if i.endswith("B3.tif")][0])    

#         b1, b2, b3 = rio.open(b1), rio.open(b2), rio.open(b3)

#         return b1, b2, b3
    
    
# from rasterio.merge import merge

# SAVE_DIR = "/sciclone/geograd/heather_data/large_test/extracted/"


# for muni in os.listdir(BASE_DIR):

#     if muni != "extracted":

#         # print(muni)
        
#         b1s, b2s, b3s = [], [], []
#         for box in os.listdir(os.path.join(BASE_DIR, muni)):

#             # print(box)
            
#             path = os.path.join(BASE_DIR, muni, box)

#             # print(path)

#             if len(os.listdir(path)) != 0:
            
#                 cur_b1, cur_b2, cur_b3 = open_tifs(str(muni), path)
#                 b1s.append(cur_b1), b2s.append(cur_b2), b3s.append(cur_b3)
                
#                 # print(path)

#         # print(b1s)
#         # print(b2s)
#         # print(b3s)
            
#         mosaic_b1, out_trans = merge(b1s)
#         mosaic_b2, out_trans = merge(b1s)
#         mosaic_b3, out_trans = merge(b1s)
        
        
#         im = np.dstack([mosaic_b1[0], mosaic_b2[0], mosaic_b3[0]])

#         imageio.imwrite(SAVE_DIR + str(muni) + ".png", im)    
