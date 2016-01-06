# -*- coding: utf-8 -*-
#******************************************************************************
#
# lesis2sqlite
# ---------------------------------------------------------
# This plugin convert lesis GIS working dir structure to sqlite data base
#
# Author:   Alexander Lisovenko, alexander.lisovenko@nextgis.ru
# *****************************************************************************
# Copyright (c) 2012-2015. NextGIS, info@nextgis.com
#
# This source is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free
# Software Foundation, either version 2 of the License, or (at your option)
# any later version.
#
# This code is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
#
# A copy of the GNU General Public License is available on the World Wide Web
# at <http://www.gnu.org/licenses/>. You can also obtain it by writing
# to the Free Software Foundation, 51 Franklin Street, Suite 500 Boston,
# MA 02110-1335 USA.
#
#******************************************************************************


import os
import sys
import time
import threading
import sqlite3

from osgeo import gdal
from osgeo import ogr, osr


def shape2sqlite(shape_file, sqlite_db_dest, getSrsWKTCallback=None, addFeatureCallback=None):
    """
        Transfer data from sape to sqlite only!
    """
    return "videl_plg"
    
    src_shape_ds = ogr.Open(shape_file)
    
    if src_shape_ds is None:
        # raise LesisExportException("OGR Data source %s cannot be opened" % shape_file)
        return None
    if src_shape_ds.GetDriver().GetName() != "ESRI Shapefile":
        # raise LesisExportException("Unexpected data source type: %s. Expected ESRI Shapefile" % src_shape_ds.GetDriver().GetName())
        return None

    if os.path.isfile(sqlite_db_dest):
        dest_ds = ogr.Open(sqlite_db_dest, True)
        
        if dest_ds is None:
            # raise LesisExportException("OGR Data source %s cannot be opened" % sqlite_db_dest)
            return None
        if dest_ds.GetDriver().GetName() != "SQLite":
            # raise LesisExportException("Unexpected data source type: %s. Expected SQLite" % dest_ds.GetDriver().GetName())
            return None
    else:
        drv = ogr.GetDriverByName("SQLite")
        dest_ds = drv.CreateDataSource(sqlite_db_dest)

        if dest_ds is None:
            # raise LesisExportException("OGR Data source %s cannot be create" % sqlite_db_dest)
            return None

    layer_src = src_shape_ds.GetLayerByIndex(0)
    src_ref = layer_src.GetSpatialRef()
    if (src_ref is None) and (getSrsWKTCallback is not None):
        src_ref = osr.SpatialReference()
        src_ref.ImportFromWkt(getSrsWKTCallback())

    layer_name = "videl_plg"
    #dest_ds.CopyLayer(layer_src, layer_name, ["OVERWRITE=YES"])
    layer_dst = dest_ds.CreateLayer(layer_name, None, layer_src.GetGeomType(), ["OVERWRITE=YES"])

    # adding fields to new layer
    layer_definition = ogr.Feature(layer_src.GetLayerDefn())
    for i in range(layer_definition.GetFieldCount()):
        layer_dst.CreateField(layer_definition.GetFieldDefnRef(i))

    layer_src.ResetReading()
    # adding the features from input to dest
    feature_count = layer_src.GetFeatureCount()
    feature_count = 5
    for i in range(0, feature_count):
        print "feature: %d" % i
        feature = layer_src.GetNextFeature()
        # feature = layer_src.GetFeature(i)
        layer_dst.CreateFeature(feature)
        if addFeatureCallback is not None:
            addFeatureCallback(i, feature_count)

        time.sleep(0.01)
    layer_dst.SyncToDisk()

    dest_ds.Release()
    src_shape_ds.Release()

    src_shape_ds = None
    dest_ds = None

    return layer_name

def create_new_plg_layer(src_layer_name, sqlite_db_file, kvr_plg_layer_name, kvrnums):
    if os.path.isfile(sqlite_db_file):
        dest_ds = ogr.Open(sqlite_db_file, True)
        
        if dest_ds is None:
            # raise LesisExportException("OGR Data source %s cannot be opened" % sqlite_db_file)
            return None
        if dest_ds.GetDriver().GetName() != "SQLite":
            # raise LesisExportException("Unexpected data source type: %s. Expected SQLite" % dest_ds.GetDriver().GetName())
            return None
    else:
        drv = ogr.GetDriverByName("SQLite")
        dest_ds = drv.CreateDataSource(sqlite_db_file)

        if dest_ds is None:
            # raise LesisExportException("OGR Data source %s cannot be create" % sqlite_db_file)
            return None

    layer_src = dest_ds.GetLayerByName(src_layer_name)
    featureDefn = layer_src.GetLayerDefn()
    nomkvrFieldDefn = featureDefn.GetFieldDefn(featureDefn.GetFieldIndex("nomkvr"))

    # layer_dst = dest_ds.CreateLayer( kvr_plg_layer_name, layer_src.GetSpatialRef(), ogr.wkbPolygon, ["OVERWRITE=YES"] )
    layer_dst = dest_ds.CreateLayer( kvr_plg_layer_name, None, ogr.wkbPolygon, ["OVERWRITE=YES"] )
    if layer_dst is None:
        dest_ds = None
        return False

    field_defn = ogr.FieldDefn( "plskvr", ogr.OFTReal )
    if layer_dst.CreateField ( field_defn ) != 0:
        dest_ds = None
        return False
    if layer_dst.CreateField ( nomkvrFieldDefn ) != 0:
        dest_ds = None
        return False

    for kvrnaum in kvrnums:
        uniteVidelsToKVR(layer_src, layer_dst, kvrnaum)

    layer_dst.SyncToDisk()

    dest_ds.Release()
    dest_ds = None

    return True

def uniteVidelsToKVR(ogr_layer_src, ogr_layer_dest, numkvr):
    ogr_layer_src.SetAttributeFilter("nomkvr=%s" % str(numkvr))

    result_polygon = ogr.Geometry(ogr.wkbPolygon)
    feature = ogr_layer_src.GetNextFeature()
    plskvr = feature.GetFieldAsDouble(feature.GetFieldIndex( "plsvyd" ))
    while feature is not None:
        geom = feature.GetGeometryRef()
        # print "geom: ", geom
        union_geom = result_polygon.Union(geom)

        if union_geom is not None:
            result_polygon = union_geom
        
        plskvr += feature.GetFieldAsDouble(feature.GetFieldIndex( "plsvyd" ))

        feature = ogr_layer_src.GetNextFeature()

    feat = ogr.Feature( ogr_layer_dest.GetLayerDefn())
    feat.SetGeometry(result_polygon)
    feat.SetField( "plskvr", plskvr )
    feat.SetField( "nomkvr", str(numkvr) )

    ogr_layer_dest.CreateFeature(feat)
