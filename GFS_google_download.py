import ee, os, shutil, joblib, time
import numpy as np
import pandas as pd
import wget
from zipfile import ZipFile
import rasterio
import rasterio.features
import rasterio.warp

# ee.Authenticate()
ee.Initialize()
geometry = ee.Geometry.Rectangle([[36.5, -33],
                                [40.5, -23.5]],
                                )


output = 'output'
path_nwp = '/media/smartrue/HHD2/GFS/Azores'
path_temp = '/media/smartrue/HHD2/GFS/temp'
if not os.path.exists(path_temp):
    os.makedirs(path_nwp)
if not os.path.exists(path_temp):
    os.makedirs(path_temp)

dates = pd.date_range(pd.to_datetime('09012018',format='%d%m%Y'), pd.to_datetime('30092020',format='%d%m%Y'))

for d in dates:
    shutil.rmtree(path_temp)
    if not os.path.exists(path_temp):
        os.makedirs(path_temp)
    dataset = ee.FeatureCollection('NOAA/GFS0P25').filter(ee.Filter.date(d.strftime('%Y-%m-%d'), (d+pd.DateOffset(days=1)).strftime('%Y-%m-%d'))).filterBounds(geometry)
    # mask out cloud covered regions

    # get image informaiton
    count = dataset.size().getInfo()
    sceneList = dataset.aggregate_array('system:index').getInfo()
    print(count)
    print(sceneList)

    nwps = dict()
    # Loop to output each image
    for i in range(0, count):
        scenename = 'NOAA/GFS0P25/' + sceneList[i]
        date, hor = sceneList[i].split('F')
        nhor = int(hor)
        upd = int(date[-2:])
        print(upd)
        if nhor<96 and upd==18:
            date_upd = date
            date = (pd.to_datetime(date[:-2], format='%Y%m%d') + pd.DateOffset(hours=nhor)).strftime('%d%m%y%H%M')
            if not date_upd in nwps.keys():
                nwps[date_upd]=dict()
            if not date in nwps[date_upd].keys():
                nwps[date_upd][date] = dict()
            layer = ee.Image(scenename).clip(geometry)
            url = layer.getDownloadURL( params={'name': 'output','region': geometry, 'crs': 'EPSG:4326', 'crs_transform': [0.25, 0, -180.125, 0, -0.25, 90.125]})
            count = 0
            while count<3:
                try:
                    print('Download...', date_upd)
                    wget.download(url, out=path_temp)
                    print('sleep...5')
                    time.sleep(5)
                    break
                except:
                    time.sleep(30)
                    print('sleep...30')
                    count+=1
                    continue
            print('Unzip...', date_upd)
            zf = ZipFile(os.path.join(path_temp,'output.zip'), 'r')
            zf.extractall(path_temp)
            zf.close()
            files=[['output.downward_shortwave_radiation_flux.tif', 'Flux'], ['output.relative_humidity_2m_above_ground.tif','Humid'],
                   ['output.temperature_2m_above_ground.tif','Temp'], ['output.total_cloud_cover_entire_atmosphere.tif', 'Cloud'],
                   ['output.u_component_of_wind_10m_above_ground.tif', 'Uwind'], ['output.v_component_of_wind_10m_above_ground.tif','Vwind']
                   ]
            print('Extract...', date_upd)
            nwps[date_upd][date]['lat'] = np.arange(36.5, 40.5, 0.25).reshape(-1,1)
            nwps[date_upd][date]['long'] = np.arange(-33, -23.5, 0.25).reshape(-1,1).T
            for file in files:
                try:
                    with rasterio.open(os.path.join(path_temp, file[0])) as data:
                        nwps[date_upd][date][file[1]] = np.array(data.read(1))
                except:
                    nwps[date_upd][date][file[1]] = np.array([])
    print('Write...' + d.strftime('%Y%m%d'))
    joblib.dump(nwps, os.path.join(path_nwp, 'gfs_' + d.strftime('%Y%m%d') + '.pickle'))