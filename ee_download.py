# -*- coding: utf-8 -*-
import calendar
import geemap
import ee
from datetime import date, timedelta
import pandas as pd
import os

#  авторизация в GEE
ee.Authenticate()
ee.Initialize(project = 'iwp-dev-383806')


def setGeom(*coords):

    # границы по пространству
    if not coords:
        minLon = 96.5
        maxLon = 114
        minLat = 46.5
        maxLat = 57  # задаём охват нужных данных - по умолчанию на Байкал
    else:
        minLon = coords[0]
        maxLon = coords[1]
        minLat = coords[2]
        maxLat = coords[3]
    geom = ee.Geometry.Polygon([[[minLon, minLat],
                                 [minLon, maxLat],
                                 [maxLon, maxLat],
                                 [maxLon, minLat],
                                 [minLon, minLat]]])  # создаём геометрию области интересов
    feature = ee.Feature(geom, {})  # создаём пространственный объект без атрибутов
    geom = feature.geometry()
    # map = geemap.Map()
    # map.centerObject(geom, zoom = 5) # Задаём местоположение и масштаб
    # map.addLayer(geom)
    # map # Извлекаем геометрию, добавляем слой на карту
    return geom


def getCMIP6(dateStart, dateEnd, geom, collection, home):
    '''

    :param dateStart:
    :param dateEnd:
    :param geom:
    :param collection:
    :return:
    '''


    if not collection:
        collection = 'NASA/GDDP-CMIP6'  # Выбираем нужный набор данных
    period = [str(dateStart), str(dateEnd)]  # задаём начало и конец периода
    bands = {'temp': 'tas',
             'tempmin': 'tasmin',
             'tempmax': 'tasmax',
             'prec': 'pr',
             'wind': 'sfcWind'}  # выбираем переменную
    print('Запрашиваем данные ', collection, ' по охвату ', geom, ' за ', dateStart, ' - ', dateEnd)
    for var, band in bands.items():
        coll = ee.ImageCollection(collection).filterBounds(geom).filterDate(period[0], period[1]).select(band)
        path = os.path.join(home, var)  # директория для скачивания
        geemap.ee_export_image_collection(coll, path,
                                          region=geom)  # экспортируем в нужную директорию для заданной области интересов


if __name__ == "__main__":
    getCMIP6('1950-01-01', '1950-12-31', setGeom(), 'NASA/GDDP-CMIP6', 'd:/CMIP6/krym')

