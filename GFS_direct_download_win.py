import numpy as np
import pandas as pd
import wget, os, time
import cfgrib, shutil, joblib
from joblib import Parallel, delayed

def download(d, output,path_nwp, path_temp, upds):
    nwps = dict()

    path_out = os.path.join(path_temp, 'GFS_'+d.strftime('%Y%m%d'))
    if not os.path.exists(path_out):
        os.makedirs(path_out)
    for upd in upds:
        for hor in range(3, 76, 3):
            d1 = pd.to_datetime(d.strftime('%Y%m%d'), format='%Y%m%d')
            date_upd = (d1 + pd.DateOffset(hours=upd)).strftime('%Y%m%d%H')
            date = (d1 + pd.DateOffset(hours=hor)).strftime('%d%m%y%H%M')
            if not date_upd in nwps.keys():
                nwps[date_upd] = dict()
            if not date in nwps[date_upd].keys():
                nwps[date_upd][date] = dict()
            if d <= pd.to_datetime('17052020', format='%d%m%Y'):
                url = 'https://www.ncei.noaa.gov/data/global-forecast-system/access/historical/forecast/grid-004-0.5-degree/' + d.strftime(
                    '%Y%m') + '/' + d.strftime('%Y%m%d') + \
                      '/gfs_4_' + d.strftime('%Y%m%d') + '_' + str(upd).zfill(2) + '00_' + str(hor).zfill(3) + '.grb2'
            else:
                url = 'www.ncei.noaa.gov/data/global-forecast-system/access/grid-004-0.5-degree/forecast/' + d.strftime(
                    '%Y%m') + '/' + d.strftime('%Y%m%d') + \
                      '/gfs_4_' + d.strftime('%Y%m%d') + '_' + str(upd).zfill(2) + '00_' + str(hor).zfill(3) + '.grb2'

            fname = 'gfs_4_' + d.strftime('%Y%m%d') + '_' + str(upd).zfill(2) + '00_' + str(hor).zfill(3) + '.grb2'
            print('Download....', date)

            count = 0
            while count < 3:
                try:
                    wget.download(url, out=path_out)
                    f = url.split(sep='/')[-1]
                    for fnme in os.listdir('.'):
                        if fnme.startswith(f[:5]):
                            os.remove(fnme)
                    break
                except:
                    time.sleep(120)
                    count += 1
                    continue
            try:
                print('write wind')
                dataforU100 = cfgrib.open_dataset(os.path.join(path_out, fname), backend_kwargs={
                    'filter_by_keys': {'cfVarName': 'u100', 'typeOfLevel': 'heightAboveGround'}})
                dataforV100 = cfgrib.open_dataset(os.path.join(path_out, fname), backend_kwargs={
                    'filter_by_keys': {'cfVarName': 'v100', 'typeOfLevel': 'heightAboveGround'}})
                lat = dataforV100.latitude.sel(latitude=np.arange(36, 41, 0.5)).data
                long = dataforV100.longitude.sel(longitude=np.arange(360 - 33.5, 360 - 23, 0.5)).data
                Uwind = dataforU100.u100.data
                Uwind = dataforU100.u100.sel(latitude=np.arange(36, 41, 0.5),
                                             longitude=np.arange(360 - 33.5, 360 - 23, 0.5)).data
                Vwind = dataforV100.v100.data
                Vwind = dataforV100.v100.sel(latitude=np.arange(36, 41, 0.5),
                                             longitude=np.arange(360 - 33.5, 360 - 23, 0.5)).data
                wspeed = np.sqrt(np.square(Uwind) + np.square(Vwind))
                r2d = 45.0 / np.arctan(1.0)
                wdir = np.arctan2(Uwind, Vwind) * r2d + 180

                nwps[date_upd][date]['Uwind'] = Uwind
                nwps[date_upd][date]['Vwind'] = Vwind
                nwps[date_upd][date]['WS'] = wspeed
                nwps[date_upd][date]['WD'] = wdir
            except:
                nwps[date_upd][date]['Uwind'] = np.array([])
                nwps[date_upd][date]['Vwind'] = np.array([])
                nwps[date_upd][date]['WS'] = np.array([])
                nwps[date_upd][date]['WD'] = np.array([])
                print('Cannot find U and V for ', d.strftime('%d%m%y%H%M'))
                pass
            try:
                print('write Temp and dew')
                dataforTemp = cfgrib.open_dataset(os.path.join(path_out, fname), backend_kwargs={
                    'filter_by_keys': {'typeOfLevel': 'heightAboveGround'}})
                temp = dataforTemp.t2m.data
                dew = dataforTemp.d2m.data
                temp = dataforTemp.t2m.sel(latitude=np.arange(36, 41, 0.5),
                                           longitude=np.arange(360 - 33.5, 360 - 23, 0.5)).data
                dew = dataforTemp.d2m.sel(latitude=np.arange(36, 41, 0.5),
                                          longitude=np.arange(360 - 33.5, 360 - 23, 0.5)).data
                lat = dataforTemp.latitude.sel(latitude=np.arange(36, 41, 0.5)).data
                long = dataforTemp.longitude.sel(longitude=np.arange(360 - 33.5, 360 - 23, 0.5)).data
                nwps[date_upd][date]['Temperature'] = temp
                nwps[date_upd][date]['DewTemp'] = dew
            except:
                nwps[date_upd][date]['Temperature'] = np.array([])
                nwps[date_upd][date]['DewTemp'] = np.array([])
                print('Cannot find Temp and Dew for ', d.strftime('%d%m%y%H%M'))
                pass
            try:
                print('write cloud')
                dataforCloud = cfgrib.open_dataset(os.path.join(path_out, fname),
                                                   backend_kwargs={'filter_by_keys': {'typeOfLevel': 'atmosphere'}})
                cloud = dataforCloud.tcc.data
                cloud = dataforCloud.tcc.sel(latitude=np.arange(36, 41, 0.5),
                                             longitude=np.arange(360 - 33.5, 360 - 23, 0.5)).data
                lat = dataforCloud.latitude.sel(latitude=np.arange(36, 41, 0.5)).data
                long = dataforCloud.longitude.sel(longitude=np.arange(360 - 33.5, 360 - 23, 0.5)).data
                nwps[date_upd][date]['Cloud'] = cloud
            except:
                nwps[date_upd][date]['Cloud'] = np.array([])
                print('Cannot find Cloud for ', d.strftime('%d%m%y%H%M'))
                pass
            try:
                print('write flux')
                dataforflux = cfgrib.open_dataset(os.path.join(path_out, fname),
                                                  backend_kwargs={
                                                      'filter_by_keys': {'stepType': 'avg', 'cfVarName': 'unknown',
                                                                         'typeOfLevel': 'surface'}})
                flux = dataforflux.dswrf.data
                flux = dataforflux.dswrf.sel(latitude=np.arange(36, 41, 0.5),
                                             longitude=np.arange(360 - 33.5, 360 - 23, 0.5)).data
                lat = dataforflux.latitude.sel(latitude=np.arange(36, 41, 0.5)).data
                long = dataforflux.longitude.sel(longitude=np.arange(360 - 33.5, 360 - 23, 0.5)).data
                nwps[date_upd][date]['Flux'] = flux
            except:
                nwps[date_upd][date]['Flux'] = np.array([])
                print('Cannot find Flux for ', d.strftime('%d%m%y%H%M'))
                pass
            try:
                dataforPrec = cfgrib.open_dataset(os.path.join(path_out, fname), backend_kwargs={
                    'filter_by_keys': {'stepType': 'avg', 'cfVarName': 'prate', 'typeOfLevel': 'surface'}})
                nPrec = dataforPrec.prate.data
                nPrec = dataforPrec.prate.sel(latitude=np.arange(36, 41, 0.5),
                                              longitude=np.arange(360 - 33.5, 360 - 23, 0.5)).data
                nwps[date_upd][date]['Precipitation'] = nPrec
                lat = dataforPrec.latitude.sel(latitude=np.arange(36, 41, 0.5)).data
                long = dataforPrec.longitude.sel(longitude=np.arange(360 - 33.5, 360 - 23, 0.5)).data
            except:
                nwps[date_upd][date]['Precipitation'] = np.array([])
                print('Cannot find Prec for ', d.strftime('%d%m%y%H%M'))
                pass
            nwps[date_upd][date]['lat'] = lat
            nwps[date_upd][date]['long'] = long
    print('Write....', d.strftime('%d%m%y%H%M'))
    shutil.rmtree(path_out)
    joblib.dump(nwps, os.path.join(path_nwp, 'gfs_' + d.strftime('%Y%m%d') + '.pickle'))
if __name__ == '__main__':

    output = 'output'
    path_nwp = 'D:/Dropbox/GFS_05/Azores'
    path_temp = 'C:/GFS_05/temp'

    if not os.path.exists(path_nwp):
        os.makedirs(path_nwp)
    if not os.path.exists(path_temp):
        os.makedirs(path_temp)
    area = [[36.5, -33], [40.5, -23.5]]
    upds = [0, 6, 12, 18]
    dates = pd.date_range(pd.to_datetime('01082019',format='%d%m%Y'), pd.to_datetime('12112020',format='%d%m%Y'))
    nwps = dict()
    res = Parallel(n_jobs=4)(delayed(download)(d, output,path_nwp, path_temp, upds) for d in dates)
    # for d in dates:
    #     download(d, output,path_nwp, path_temp, upds)