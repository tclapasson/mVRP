SET OSGEO4W_ROOT=C:\OSGeo4W64
call "%OSGEO4W_ROOT%"\bin\o4w_env.bat

@echo off
path %PATH%;%OSGEO4W_ROOT%\apps\qgis-ltr\bin
path %PATH%;%OSGEO4W_ROOT%\apps\grass\grass78\lib
path %PATH%;C:\OSGeo4W64\apps\Qt5\bin
path %PATH%;C:\OSGeo4W64\apps\Python39\Scripts
path %PATH%;C:\OSGeo4W64\apps\Python39\Lib\site-packages
path %PATH%;C:/OSGeo4W/apps/qgis-ltr/python/plugins
path %PATH%;C:/OSGeo4W/apps/qgis-ltr/python
path %PATH%;C:/OSGeo4W/apps/qgis-ltr/python/plugins
path %PATH%;C:/Users/tclapasson/AppData/Roaming/QGIS/QGIS3/profiles/default/python/plugins


set PYTHONPATH=%PYTHONPATH%;%OSGEO4W_ROOT%\apps\qgis-ltr\python
set PYTHONHOME=%OSGEO4W_ROOT%\apps\Python39


start "PyCharm aware of Quantum GIS" /B "C:\Program Files\JetBrains\PyCharm Community Edition 2022.2.2\bin\pycharm64.exe"