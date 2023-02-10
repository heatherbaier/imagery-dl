from mpi4py import MPI

import geopandas as gpd
import calendar
import pygee
import os

from utils import *


def chunker_list(seq, size):
    return (seq[i::size] for i in range(size))


if __name__ == "__main__":

    BASE_DIR = "/sciclone/geograd/heather_data/slv_0713/"

    # Set up the MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()    

    print(rank, size)

    # with open("/sciclone/geograd/Heather/temporal_mex/data/missing_munis.txt", "r") as f:
    #     missing_munis = f.read().splitlines()

    if rank == 0:
        
        all_dates = []

        for year in range(1992, 2007):

            year_direc = os.path.join(BASE_DIR, str(year))
            if not os.path.isdir(year_direc):
                os.mkdir(year_direc)            

            for month in range(8, 12):

                month_direc = os.path.join(BASE_DIR, str(year), str(month))
                if not os.path.isdir(month_direc):
                    os.mkdir(month_direc)                

                all_dates.append([str(year), str(month)])

        print(all_dates)

    else:

        all_dates = None

    all_dates = comm.scatter(all_dates, root = 0)


    # gdf = gpd.read_file("/sciclone/geograd/Heather/temporal_mex/data/ipumns_bbox.shp")
    # gdf = gdf[gdf['shapeID'].isin(missing_munis)]

    shp = gpd.read_file("/sciclone/geograd/Heather/temporal_slv/data/geo2_sv1992_2007.shp")
    # shp = shp[shp["GEOLEVEL2"].isin(missing_munis)]
    shp = shp.dropna(subset = ['geometry'])
    # shp = shp.set_crs("EPSG:4326")
    shp = shp.to_crs("EPSG:6362")
    shp = pygee.remove_islands(shp, "GEOLEVEL2")
    shp = shp.to_crs("EPSG:4326")
    
    gdf = pygee.convert_to_bbox(shp)
    gdf = gdf.rename(columns = {"GEOLEVEL2": "shapeID"})

    with open("/sciclone/home20/hmbaier/temporal_mex/log_0713.txt", "a") as f:
        f.write("Number of polys in shp (no bounds): " + str(len(shp)) + "\n")

    with open("/sciclone/home20/hmbaier/temporal_mex/log_0713.txt", "a") as f:
        f.write("Number of polys in shp (bounds): " + str(len(gdf)) + "\n")        

    # shp = shp[shp['GEOLEVEL2'].isin(missing_munis)]    

    IC = "LANDSAT/LT05/C02/T1_L2"
    BANDS = ['SR_B3', 'SR_B2', 'SR_B1']
    RANK_DIREC = os.path.join(BASE_DIR, all_dates[0], all_dates[1])

    i = 0
    for col, row in gdf.iterrows():
        
        dates = pygee.GetDays(all_dates[0], all_dates[1])

        if str(row.shapeID) not in os.listdir(RANK_DIREC):

        # if str(row.shapeID) in missing_munis:

            # tiles = pygee.Tiler(shapeID = str(row.shapeID),
            #                 shp = shp,
            #                 bbox_shp = gdf,
            #                 crs = "EPSG:6362").int_grid

            # # temp = gdf[gdf['shapeID'] == row.shapeID]
            # # temp = temp.set_crs("EPSG:4326")
            # # temp = temp.to_crs("EPSG:6362")

            # # partitions = katana(temp["geometry"].to_list()[0], 50000)

            # print(str(row.shapeID), " has ", len(tiles), " partitions.")

            # # bean = gpd.GeoDataFrame(geometry = partitions)
            # # bean['id'] = [i for i in range(0, len(bean))]
            # # bean = bean.set_crs("EPSG:6362")
            # # bean = bean.to_crs("EPSG:4326")
            # # tiles.to_file(os.path.join(BASE_DIR, "shps", str(row.shapeID) + ".shp"))

            # temp_dir = os.path.join(RANK_DIREC, str(row.shapeID))
            # os.mkdir(temp_dir)

            # for tile_col, tile_row in tiles.iterrows():

            #     pygee.download_imagery(geom = tile_row.geometry,
            #                             shapeID = row.shapeID + "_box" + str(tile_row.shapeID),
            #                             ic = IC, 
            #                             dates = dates, 
            #                             imagery_dir = temp_dir, 
            #                             bands = BANDS)

            # else:

            try:

                pygee.download_imagery(geom = row.geometry,
                                    shapeID = row.shapeID,
                                    ic = IC, 
                                    dates = dates, 
                                    imagery_dir = RANK_DIREC, 
                                    bands = BANDS)

                pygee.save_pngs(shapeID = row.shapeID,
                                base_dir = RANK_DIREC,
                                export_dir = os.path.join(BASE_DIR, "extracted"),
                                l = 5)

            except Exception as e:

                print(e)

            i += 1





    # gdf = gpd.read_file("/sciclone/geograd/Heather/temporal_mex/data/ipumns_bbox.shp")
    # gdf.head()

    # IC = "LANDSAT/LT05/C01/T1"
    # BANDS = ["B1", "B2", "B3"]
    # BASE_DIR = "/sciclone/geograd/heather_data/temporal_mex/"

    # for year in range(2005, 2010):
        
    #     year_direc = os.path.join(BASE_DIR, str(year))
    #     if not os.path.isdir(year_direc):
    #         os.mkdir(year_direc)
        
    #     for month in range(1, 13):
            
    #         month_direc = os.path.join(BASE_DIR, str(year), str(month))
    #         if not os.path.isdir(month_direc):
    #             os.mkdir(month_direc)
                
    #         i = 0
    #         for col, row in gdf.iterrows():
                
    #             dates = GetDays(year, month)

    #             try:

    #                 pygee.download_imagery(geom = row.geometry,
    #                                     shapeID = row.shapeID,
    #                                     ic = IC, 
    #                                     dates = dates, 
    #                                     imagery_dir = month_direc, 
    #                                     bands = BANDS)
    #             except:

    #                 pass

    #             with open("./counter.txt", "w") as f:
    #                 f.write(str(i) + " out of " + str(len(gdf)))

    #             i += 1
                    
            
    #         print(dates)