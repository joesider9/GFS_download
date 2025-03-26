import numpy as np
import pandas as pd
import wget, os, time
import pygrib, shutil, joblib

output = 'output'
path_nwp = '/media/smartrue/HHD2/GFS_05/Azores'
path_temp = '/media/smartrue/HHD2/GFS_05/temp'

if not os.path.exists(path_temp):
    os.makedirs(path_nwp)
if not os.path.exists(path_temp):
    os.makedirs(path_temp)
area = [[36.5, -33], [40.5, -23.5]]
upds = [0, 6, 12, 18]
dates = pd.date_range(pd.to_datetime('11082019',format='%d%m%Y'), pd.to_datetime('12112020',format='%d%m%Y'))
nwps = dict()

for d in dates:
    shutil.rmtree(path_temp)
    if not os.path.exists(path_temp):
        os.makedirs(path_temp)
    for upd in upds:
        for hor in range(0, 76, 3):
            d1 = pd.to_datetime(d.strftime('%Y%m%d'), format='%Y%m%d')
            date_upd = (d1+pd.DateOffset(hours=upd)).strftime('%Y%m%d%H')
            date = d.strftime('%d%m%y%H%M')
            if not date_upd in nwps.keys():
                nwps[date_upd]=dict()
            if not date in nwps[date_upd].keys():
                nwps[date_upd][date] = dict()
            if d<=pd.to_datetime('17052020',format='%d%m%Y'):
                url = 'https://www.ncei.noaa.gov/data/global-forecast-system/access/historical/forecast/grid-004-0.5-degree/'+d.strftime('%Y%m')+'/'+d.strftime('%Y%m%d')+\
                  '/gfs_4_'+d.strftime('%Y%m%d')+ '_'+ str(upd).zfill(2) +'00_'+ str(hor).zfill(3) +'.grb2'
            else:
                url = 'www.ncei.noaa.gov/data/global-forecast-system/access/grid-004-0.5-degree/forecast/'+d.strftime('%Y%m')+'/'+d.strftime('%Y%m%d')+\
                  '/gfs_4_'+d.strftime('%Y%m%d')+ '_'+ str(upd).zfill(2) +'00_'+ str(hor).zfill(3) + '.grb2'

            fname = 'gfs_4_' + d.strftime('%Y%m%d') + '_' + str(upd).zfill(2) + '00_' + str(hor).zfill(3) + '.grb2'
            # if not os.path.exists(os.path.join(path_temp, fname)):
            print('Download....', date)

            count = 0
            while count < 3:
                try:
                    wget.download(url, out=path_temp)
                    break
                except:
                    time.sleep(120)
                    count += 1
                    continue

            grb = pygrib.open(os.path.join(path_temp, fname))
            messages = []
            nU=None; nV = None; nTemp = None; nPrec = None; nCloud = None; nFlux=None; nDew = None
            for m in range(1, grb.messages + 1):
                messages.append((m, grb.message(m).name, grb.message(m).level, grb.message(m).typeOfLevel,
                         grb.message(m).topLevel, grb.message(m).productType))
                g = grb.message(m)
                print(m)
                print(g.name)
                if g.name == '10 metre U wind component':
                    nU = m
                elif g.name == '10 metre V wind component':
                    nV = m
                elif g.name == '2 metre temperature':
                    nTemp = m
                elif g.name == 'Total Precipitation':
                    nPrec = m
                elif g.name == 'Total Cloud Cover':
                    nCloud = m
                elif g.name == 'Downward short-wave radiation flux':
                    nFlux = m
                elif g.name == '2 metre dewpoint temperature':
                    nDew = m

            messages = pd.DataFrame(messages)
            messages.to_csv(os.path.join(path_nwp, 'messages' + d.strftime('%Y%m%d') +'.csv'))
            la1 = area[0][0]
            la2 = area[1][0]
            lo1 = area[0][1]
            lo2 = area[1][1]
            print('write wind')
            if (not nU is None) and (not nV is None):
                g = grb.message(nU)
                if g.name == '10 metre U wind component':
                    Uwind, lat, long = g.data(lat1=la1, lat2=la2, lon1=lo1, lon2=lo2)


                g = grb.message(nV)
                if g.name == '10 metre V wind component':
                    Vwind = g.data(lat1=la1, lat2=la2, lon1=lo1, lon2=lo2)[0]

                r2d = 45.0 / np.arctan(1.0)
                wspeed = np.sqrt(np.square(Uwind) + np.square(Vwind))
                wdir = np.arctan2(Uwind, Vwind) * r2d + 180


                nwps[date_upd][date]['Uwind'] = Uwind
                nwps[date_upd][date]['Vwind'] = Vwind
                nwps[date_upd][date]['WS'] = wspeed
                nwps[date_upd][date]['WD'] = wdir
            else:
                nwps[date_upd][date]['Uwind'] = np.array([])
                nwps[date_upd][date]['Vwind'] = np.array([])
                nwps[date_upd][date]['WS'] = np.array([])
                nwps[date_upd][date]['WD'] = np.array([])
                print('Cannot find U and V for ', d.strftime('%d%m%y%H%M'))
            print('write Temp')
            if not nTemp is None:
                g = grb.message(nTemp)
                if g.name == '2 metre temperature':
                    x, lat, long= g.data(lat1=la1, lat2=la2, lon1=lo1, lon2=lo2)
                    nwps[date_upd][date]['Temperature'] = x
            else:
                nwps[date_upd][date]['Temperature'] = np.array([])
                print('Cannot find Temp for ', d.strftime('%d%m%y%H%M'))
            print('write Prec')
            if not nPrec is None:
                g = grb.message(nPrec)
                if g.name == 'Total Precipitation':
                    x, lat, long = g.data(lat1=la1, lat2=la2, lon1=lo1, lon2=lo2)
                    nwps[date_upd][date]['Precipitation'] = x
            else:
                nwps[date_upd][date]['Precipitation'] = np.array([])
                print('Cannot find Prec for ', d.strftime('%d%m%y%H%M'))
            print('write Cloud')
            if not nCloud is None:
                g = grb.message(nCloud)
                if g.name == 'Total Cloud Cover':
                    x, lat, long = g.data(lat1=la1, lat2=la2, lon1=lo1, lon2=lo2)
                    nwps[date_upd][date]['Cloud'] = x
            else:
                nwps[date_upd][date]['Cloud'] = np.array([])
                print('Cannot find Cloud for ', d.strftime('%d%m%y%H%M'))
            print('write Flux')
            if not nFlux is None:
                g = grb.message(nFlux)
                if g.name == 'Downward short-wave radiation flux':
                    x, lat, long = g.data(lat1=la1, lat2=la2, lon1=lo1, lon2=lo2)
                    nwps[date_upd][date]['Flux'] = x
            else:
                nwps[date_upd][date]['Flux'] = np.array([])
                print('Cannot find Flux for ', d.strftime('%d%m%y%H%M'))
            print('write Dew')
            if not nDew is None:
                g = grb.message(nDew)
                if g.name == '2 metre dewpoint temperature':
                    x, lat, long = g.data(lat1=la1, lat2=la2, lon1=lo1, lon2=lo2)
                    nwps[date_upd][date]['DewTemp'] = x
            else:
                nwps[date_upd][date]['DewTemp'] = np.array([])
                print('Cannot find Dew  for ', d.strftime('%d%m%y%H%M'))

            nwps[date_upd][date]['lat'] = lat
            nwps[date_upd][date]['long'] = long
            del x
            print('Done')
    print('Write....', d.strftime('%d%m%y%H%M'))
    joblib.dump(nwps, os.path.join(path_nwp, 'gfs_' + d.strftime('%Y%m%d') + '.pickle'))