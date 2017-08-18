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
import time
import threading
import sqlite3

from osgeo import ogr, osr

from dbfread import DBF

from shape2sqlite import shape2sqlite, create_new_plg_layer

LESIS_ENCODING = "866"

typemap = {
    'F': 'FLOAT',
    'L': 'BOOLEAN',
    'I': 'INTEGER',
    'C': 'TEXT',
    'N': 'REAL',  # because it can be integer or float
    'M': 'TEXT',
    'D': 'DATE',
    'T': 'DATETIME',
    '0': 'INTEGER',
}

class LesisExportException(Exception):
    def __init__(self, message):
        super(LesisExportException, self).__init__(message)


class LesisFileStructure(object):
    def __init__(self, base_dir):
        if not os.path.isdir(base_dir):
            raise ValueError("Specified directory not exist")

        self.__base_dir = base_dir
        self.__reference_data_dirs = []
        self.__data_dirs = []

        for dirpath, dirnames, filenames in os.walk(self.__base_dir):
            if ('n' in dirnames) or ('N' in dirnames):
                self.__reference_data_dirs.insert(0, os.path.join(dirpath, 'n'))
            if ('d' in dirnames) or ('D' in dirnames):
                self.__data_dirs.append(os.path.join(dirpath, 'd'))

        self.__fields_dbf = os.path.join(self.__reference_data_dirs[-1], "Fields.DBF")
        if  not os.path.isfile(self.__fields_dbf):
            self.__fields_dbf = None
            raise ValueError("Fields.DBF not found!")

    def getDBFbyName(self, dbf_name):
        for reference_data_dir in self.__reference_data_dirs:
            for f in os.listdir(reference_data_dir):
                if os.path.splitext(f)[1].lower() != ".dbf":
                    continue

                if os.path.splitext(f)[0].lower() == os.path.splitext(dbf_name)[0].lower():
                    return os.path.join(reference_data_dir, f)
        return None

    def getFieldsDBFFile(self):
        return self.__fields_dbf

    def getPHLDataFiles(self):
        dataFiles = []
        for data_dir in self.__data_dirs:
            dataFilesSet = {}
            for f in os.listdir(data_dir):
                if f.lower()  == "phl1.dbf":
                    dataFilesSet.update({"phl1": os.path.join(data_dir, f)})
                if f.lower()  == "phl2.dbf":
                    dataFilesSet.update({"phl2": os.path.join(data_dir, f)})
                if f.lower()  == "phl3.dbf":
                    dataFilesSet.update({"phl3": os.path.join(data_dir, f)})
            if len(dataFilesSet) > 0:
                dataFiles.append(dataFilesSet)

        return dataFiles


def getFieldsDescFromDBF(dbf_fields_file):
    dbf_table = DBF(
        dbf_fields_file,
        lowernames=True,
        encoding=LESIS_ENCODING
    )

    fields_desc = {}
    for rec in dbf_table:
        field_desc = {
            rec.get(u'field').lower():{
                "type": rec.get(u'tip'),
                "name": rec.get(u'name'),
                "ref":  rec.get(u'sprav'),
            }
        }

        fields_desc.update(field_desc)

    return fields_desc

# reference_cash = {}

# def videlsCount(sqlite_db_file, tbl_name):
#     conn = sqlite3.connect(sqlite_db_file)
#     cur = conn.cursor()

#     cur.execute("select count(*) from %s" % tbl_name)
#     videl_count = cur.fetchone()[0]
#     return videl_count

# def processPHL1(phl1_dbf_file, tbl_name, sqlite_db_file, fields_desc, lesis_file_structure, subscriber_func = None):
    
#     def getDBFValue(v):
#         '''
#             dbf_file_name
#             dbf_file_value
#         '''
#         dbf_file_name = v[0]
#         dbf_file_value = unicode(v[1])

#         field_desc = fields_desc.get(dbf_file_name)

#         if field_desc[u"ref"] in ["", None]:
#             return v[1] 

#         ref_dbf_file = lesis_file_structure.getDBFbyName(field_desc[u"ref"])
#         if ref_dbf_file is None:
#             return u"Reference file not found!"

#         try:
#             return reference_cash[(ref_dbf_file, dbf_file_value)]

#         except KeyError:
#             ref_dbf_table = DBF(
#                 ref_dbf_file,
#                 lowernames=True,
#                 encoding=LESIS_ENCODING
#             )
            
#             kl_type = u"N"
#             for dbf_field in ref_dbf_table.fields:
#                 if dbf_field.name == u"kl":
#                     kl_type = dbf_field.type

#             is_found = False
#             for row in ref_dbf_table:
#                 # TODO get find KL and TX field exception
#                 kl = row.get(u"kl")
#                 if kl_type in [u"I", u"F", u"N", u"0"] and kl is None:
#                     kl = 0
#                 kl = unicode(kl)
#                 tx = row.get(u"tx", None)

#                 if dbf_file_value == kl:
#                     is_found = True
#                     kl = dbf_file_value

#                 reference_cash.update({(ref_dbf_file, kl): tx})

#             if not is_found:
#                 reference_cash.update({(ref_dbf_file, dbf_file_value): None})

#             return (dbf_file_name, reference_cash[(ref_dbf_file, dbf_file_value)] )

#     dbf_table = DBF(
#         phl1_dbf_file,
#         lowernames=True,
#         encoding=LESIS_ENCODING
#     )

#     conn = sqlite3.connect(sqlite_db_file)
#     cur = conn.cursor()

#     cur.execute("PRAGMA table_info(%s)" % tbl_name)
#     existing_fileds = [field[1] for field in cur.fetchall()]

#     export_fields = []
#     for export_field in dbf_table.fields:
#         field_name = export_field.name
#         field_type = typemap.get(export_field.type, 'TEXT')

#         if field_name in existing_fileds:
#             continue

#         field_desc = fields_desc.get(export_field.name, None)
#         if field_desc is None:
#             continue

#         if field_desc[u"ref"] not in ["", None]:
#             field_type = 'TEXT'

#         sql = "alter table %s add column %s %s" % (tbl_name, field_name, field_type)
#         cur.execute(sql)
#         export_fields.append(field_name)

#     conn.commit()

#     exceptions = []

#     videl_count = dbf_table._count_records()
#     videl_index = 1
#     for row in dbf_table:
        
#         nomkvr = row.get(u"nomkvr", None)
#         nomvyd = row.get(u"nomvyd", None)
        
#         if nomkvr is None or nomvyd is None:
#             continue
        
#         values = [(v[0], getDBFValue(v)) for v in row.items() if v[0] in export_fields]

#         try:
#             sql = "update %s set %s where nomkvr = %s and nomvyd = %s" % (
#                 tbl_name,
#                 ", ".join(["%s=?" % v[0] for v in values]),
#                 nomkvr,
#                 nomvyd   
#             )

#             cur.execute(sql, [v[1] for v in values])
#             subscriber_func(videl_index, videl_count)
#             videl_index += 1
#         except Exception as err:
#             exceptions.append("Update %s where nomkvr = %s and nomvyd = %s error: %s" % (
#                 tbl_name,
#                 nomkvr,
#                 nomvyd,
#                 str(err)
#             ))

#         #print("----- %s seconds ---" % (time.time() - start_time_1))

#     conn.commit()
#     return exceptions

# def processPHL2(phl1_dbf_file, layer_name, sqlite_db_file, fields_desc):
#     pass


# def addKVRpoligonLayer(layer_name, sqlite_db_file, kvr_plg_layer_name="kvr_plg"):
#     conn = sqlite3.connect(sqlite_db_file)
#     cur = conn.cursor()

#     sql = "select DISTINCT nomkvr from %s" % layer_name
#     cur.execute(sql)
    
#     kvrs_info = cur.fetchall()

#     cur.close()
#     conn.close()

#     create_new_plg_layer(
#         layer_name,
#         sqlite_db_file,
#         kvr_plg_layer_name,
#         [kvr_info[0] for kvr_info in kvrs_info]
#     )


# def parse_args():
#     import argparse
#     parser = argparse.ArgumentParser(
#         description='usage: %prog lesis_export_base_dir')
#     arg = parser.add_argument

#     arg('--lesis_export_base_dir',
#         action='store',
#         dest='lesis_export_base_dir',
#         default=None,
#         help='lesis export data base dir ')

#     return parser.parse_args()


class ConverterInteruptException(Exception):
    def __init__(self, message):
        super(ConverterInteruptException, self).__init__(message)


class ConverterErrorException(Exception):
    def __init__(self, message):
        super(ConverterErrorException, self).__init__(message)


class Converter(object):
    RETURN_CODE_SUCCESS     = 0
    RETURN_CODE_ERROR       = 1
    RETURN_CODE_INTERUPT    = 2

    CONVERT_STATE_STARTED       = 0
    CONVERT_STATE_PREPARE       = 1
    CONVERT_STATE_COPY_SHAPE    = 2
    CONVERT_STATE_GEN_KVR       = 3
    CONVERT_STATE_PROCESS_PHL1  = 4
    CONVERT_STATE_PROCESS_PHL2  = 5
    CONVERT_STATE_PROCESS_PHL3  = 6
    CONVERT_STATE_CREATE_ALIASES= 7
    CONVERT_STATE_WAIT          = 98
    CONVERT_STATE_FINISHED      = 99

    CONVERT_STATE_NAMES = {
        CONVERT_STATE_WAIT:             "Wait for convertation",
        CONVERT_STATE_STARTED:          "Convert STARTED",
        CONVERT_STATE_FINISHED:         "Convert FINISHED",
        CONVERT_STATE_PREPARE:          "Convert prepare",
        CONVERT_STATE_COPY_SHAPE:       "Copy videls shape",
        CONVERT_STATE_GEN_KVR:          "Generate kvr layers",
        CONVERT_STATE_PROCESS_PHL1:     "Process PHL1.dbf",
        CONVERT_STATE_PROCESS_PHL2:     "Process PHL2.dbf",
        CONVERT_STATE_PROCESS_PHL3:     "Process PHL3.dbf",
        CONVERT_STATE_CREATE_ALIASES:   "Create aliases"
    }

    def __init__(self, lesis_base_dir, shape_filename, sqlite_filename):
        self.__lesis_base_dir   = lesis_base_dir
        self.__shape_filename   = shape_filename
        self.__sqlite_filename  = sqlite_filename

        self.__lfs = None
        self.__lessis_fields_struct = {}
        self.__reference_cash = {}
        self.__aliases = []

        self.__current_convert_status = self.CONVERT_STATE_WAIT
        self.__exceptions = []

        self.videlLayerName = "videl_plg"
        self.kvrLayerName = "kvr_plg"
        self.yarporLayerName = "yarpor_attr"
        self.alias_table_name = "aliases"

        self.maket_table_name_pattern = u"maket_%s"

        self.__status_changed_callback = None
        self.__status_set_crs = None

        self.__interupt = threading.Event()
        self.__interupt.clear()

    def setStatusChangedCallback(self, func):
        self.__status_changed_callback = func

    def setSetCRSCallback(self, func):
        self.__status_set_crs = func
    
    def interupt(self):
        self.__interupt.set()

    def getExceptions(self):
        return self.__exceptions

    def convert(self):
        self.__setStatus(self.CONVERT_STATE_STARTED)
        
        result = self.RETURN_CODE_ERROR

        if os.path.isfile(self.__sqlite_filename):
            os.remove(self.__sqlite_filename)

        try:
            self.__setStatus(self.CONVERT_STATE_PREPARE)
            self.__setStatusMessage("Start")
            self.__validateLesisWorkDir()
            self.__setStatusMessage("Finish")

            self.__setStatus(self.CONVERT_STATE_COPY_SHAPE)
            self.__setStatusMessage("Start")
            self.__copyShape()
            self.__setStatusMessage("Finish")

            self.__setStatus(self.CONVERT_STATE_GEN_KVR)
            self.__setStatusMessage("Start")
            self.__createKvrLayer()
            self.__setStatusMessage("Finish")

            self.__setStatus(self.CONVERT_STATE_PROCESS_PHL1)
            self.__setStatusMessage("Start")
            self.__processPhl1()
            self.__setStatusMessage("Finish")

            self.__setStatus(self.CONVERT_STATE_PROCESS_PHL2)
            self.__setStatusMessage("Start")
            self.__processPhl2()
            self.__setStatusMessage("Finish")

            self.__setStatus(self.CONVERT_STATE_PROCESS_PHL3)
            self.__setStatusMessage("Start")
            self.__processPhl3()
            self.__setStatusMessage("Finish")

            self.__setStatus(self.CONVERT_STATE_CREATE_ALIASES)
            self.__setStatusMessage("Start")
            self.__createAliasesTable()
            self.__setStatusMessage("Finish")

            result = self.RETURN_CODE_SUCCESS

        except ConverterErrorException as err:
            msg = self.__setStatusMessage(err.message)
            self.__exceptions.append(msg)
            result = self.RETURN_CODE_ERROR            

        except ConverterInteruptException:
            result = self.RETURN_CODE_INTERUPT

        return result

    def __setStatus(self, status):
        self.__current_convert_status = status
        self.__setStatusMessage("Set")

    def __setStatusMessage(self, status_msg):
        if self.__status_changed_callback is not None:
            msg = "%s: %s" % (
                self.CONVERT_STATE_NAMES[self.__current_convert_status],
                status_msg
            )
            self.__status_changed_callback(msg)

        return msg

    def __validateLesisWorkDir(self):
        try:
            self.__lfs = LesisFileStructure(self.__lesis_base_dir)
            
            self.__lessis_fields_struct = getFieldsDescFromDBF(
                self.__lfs.getFieldsDBFFile()
            )

            fieldsExt = {
                u"arn": {
                    "type": u"char",
                    "name": u"Арендатор",
                    "ref":  u"arnLesse",
                },
                u"vozrub": {
                    "type": u"char",
                    "name": u"Возраст рубки",
                    "ref":  None,
                }
            }
            self.__lessis_fields_struct.update(fieldsExt)
        except ValueError as err:
            self.__return_code = self.RETURN_CODE_ERROR

            msg = "Analize lesis dir struct. Error: " + str(err)
            self.__exceptions.append(msg)
            self.__setStatusMessage(msg)
            
    def __getShapeDS(self):
        shape_ds = ogr.Open(self.__shape_filename)

        if shape_ds is None:
            msg = "Error: can't create ogr datasource for shape."
            raise ConverterErrorException(msg)
        if shape_ds.GetDriver().GetName() != "ESRI Shapefile":
            msg = "Error: input file have not shape format."
            raise ConverterErrorException(msg)

        return shape_ds 

    def __getSQLiteDS(self):
        sqlite_ds = None
        
        if os.path.isfile(self.__sqlite_filename):
            sqlite_ds = ogr.Open(self.__sqlite_filename, True)
            
            if sqlite_ds is None:
                raise ConverterErrorException("Error: can't create ogr datasource for sqlite.")

            if sqlite_ds.GetDriver().GetName() != "SQLite":
                raise ConverterErrorException("Error: input file have not sqlite format.")
        else:
            drv = ogr.GetDriverByName("SQLite")
            
            # sqlite_ds = drv.CreateDataSource(self.__sqlite_filename, ["SPATIALITE=YES"])
            sqlite_ds = drv.CreateDataSource(self.__sqlite_filename)

            if sqlite_ds is None:
                raise ConverterErrorException(" Error: can't create ogr datasource for sqlite.")

        return sqlite_ds

    def __copyShape(self):
        src_shape_ds = self.__getShapeDS()
        dest_ds = self.__getSQLiteDS()
        
        layer_src = src_shape_ds.GetLayerByIndex(0)
        src_ref = layer_src.GetSpatialRef()

        if (src_ref is None) and (self.__status_set_crs is not None):
            src_ref = osr.SpatialReference()
            src_ref.ImportFromWkt(self.__status_set_crs())

        layer_name = self.videlLayerName
        #dest_ds.CopyLayer(layer_src, layer_name, ["OVERWRITE=YES"])
        layer_dst = dest_ds.CreateLayer(layer_name, src_ref, layer_src.GetGeomType(), ["OVERWRITE=YES"])

        # adding fields to new layer
        layer_definition = ogr.Feature(layer_src.GetLayerDefn())
        for i in range(layer_definition.GetFieldCount()):
            layer_dst.CreateField(layer_definition.GetFieldDefnRef(i))

        layer_src.ResetReading()
        feature_count = layer_src.GetFeatureCount()
        # feature_count = 10
        for i in range(0, feature_count):
            if self.__interupt.isSet():
                raise ConverterInteruptException("Interupt")

            feature = layer_src.GetNextFeature()
            # feature = layer_src.GetFeature(i)
            layer_dst.CreateFeature(feature)
            
            self.__setStatusMessage("Copied %d from %d features" % (i, feature_count))

            time.sleep(0.001)
        layer_dst.SyncToDisk()

        dest_ds.Release()
        src_shape_ds.Release()

        src_shape_ds = None
        dest_ds = None

        return layer_name

    def __createKvrLayer(self):
        conn = sqlite3.connect(self.__sqlite_filename)
        cur = conn.cursor()

        sql = "select DISTINCT nomkvr from %s" % self.videlLayerName
        cur.execute(sql)
        
        kvrs_nums = [kvr_info[0] for kvr_info in cur.fetchall()]

        cur.close()
        conn.close()

        sqlite_ds = self.__getSQLiteDS()

        layer_src = sqlite_ds.GetLayerByName(self.videlLayerName)
        featureDefn = layer_src.GetLayerDefn()

        layer_dst = sqlite_ds.CreateLayer( self.kvrLayerName, layer_src.GetSpatialRef(), ogr.wkbPolygon, ["OVERWRITE=YES"] )
        if layer_dst is None:
            msg = "Error: can't create ogr layer."
            raise ConverterErrorException(msg)

        field_defn = ogr.FieldDefn( "plskvr", ogr.OFTReal )
        if layer_dst.CreateField ( field_defn ) != 0:
            msg = "Error: can't create ogr field 'plskvr'."
            raise ConverterErrorException(msg)

        nomkvrFieldDefn = featureDefn.GetFieldDefn(featureDefn.GetFieldIndex("nomkvr"))    
        if layer_dst.CreateField ( nomkvrFieldDefn ) != 0:
            msg = "Error: can't create ogr field 'nomkvr'."
            raise ConverterErrorException(msg)

        kvrs_nums_len = len(kvrs_nums)
        kvrs_nums_processed = 0
        for kvr_num in kvrs_nums:
            self.__setStatusMessage("Process blocks %d from %d (%s)" % (kvrs_nums_processed, kvrs_nums_len, kvr_num, ))
            layer_src.SetAttributeFilter("nomkvr=%s" % str(kvr_num))

            result_polygon = ogr.Geometry(ogr.wkbPolygon)
            feature = layer_src.GetNextFeature()
            
            plskvr = 0.0

            while feature is not None:
                if self.__interupt.isSet():
                    raise ConverterInteruptException("Interupt")

                geom = feature.GetGeometryRef()
                union_geom = result_polygon.Union(geom)

                if union_geom is not None:
                    result_polygon = union_geom
                
                plskvr += feature.GetFieldAsDouble(feature.GetFieldIndex( "plsvyd" ))

                # self.__setStatusMessage("Process parsel %d" % (feature.GetFID(), ))
                # self.__setStatusMessage("Process parsel %d" % (feature.GetFID(), ))

                feature = layer_src.GetNextFeature()
                time.sleep(0.001)

            feat = ogr.Feature( layer_dst.GetLayerDefn())
            feat.SetGeometry(result_polygon)
            feat.SetField( "plskvr", plskvr )
            feat.SetField( "nomkvr", str(kvr_num) )

            layer_dst.CreateFeature(feat)

            kvrs_nums_processed += 1

        sqlite_ds.Release()
        sqlite_ds = None

    def __getDBFValueByField(self, (field_name, field_value)):
        field_desc = self.__lessis_fields_struct.get(field_name)

        if field_desc is None:
            return None

        field_desc_ref = field_desc[u"ref"]
        
        if field_desc_ref in ["", None]:
            return field_value

        ref_dbf_file = self.__lfs.getDBFbyName(field_desc_ref)
        if ref_dbf_file is None:
            return u"Reference file not found!"

        return self.__getDBFValueByDBF((ref_dbf_file, field_value))

    def __getDBFValueByDBF(self, (ref_dbf_file, field_value)):
        dbf_field_value = unicode(field_value)

        try:
            return self.__reference_cash[(ref_dbf_file, dbf_field_value)]
        except KeyError:
            ref_dbf_table = DBF(
                ref_dbf_file,
                lowernames=True,
                encoding=LESIS_ENCODING
            )
            
            kl_type = u"N"
            for dbf_field in ref_dbf_table.fields:
                if dbf_field.name == u"kl":
                    kl_type = dbf_field.type

            is_found = False
            for row in ref_dbf_table:
                # TODO get find KL and TX field exception
                kl = row.get(u"kl")
                if kl_type in [u"I", u"F", u"N", u"0"] and kl is None:
                    kl = 0
                kl = unicode(kl)
                tx = row.get(u"tx", None)

                if dbf_field_value == kl:
                    is_found = True
                    kl = dbf_field_value

                self.__reference_cash.update({(ref_dbf_file, kl): tx})

            if not is_found:
                self.__reference_cash.update({(ref_dbf_file, dbf_field_value): None})

            return self.__reference_cash[(ref_dbf_file, dbf_field_value)]

    def __processPhl1(self):
        phl1_dbf_file = self.__lfs.getPHLDataFiles()[0]['phl1']

        dbf_table = DBF(
            phl1_dbf_file,
            lowernames=True,
            encoding=LESIS_ENCODING
        )

        conn = sqlite3.connect(self.__sqlite_filename)
        cur = conn.cursor()

        cur.execute("PRAGMA table_info(%s)" % self.videlLayerName)
        existing_fileds = [field[1] for field in cur.fetchall()]

        export_fields = []
        for export_field in dbf_table.fields:
            field_name = export_field.name
            field_type = typemap.get(export_field.type, 'TEXT')

            if field_name in existing_fileds:
                continue

            field_desc = self.__lessis_fields_struct.get(field_name, None)
            if field_desc is None:
                continue

            if field_desc[u"ref"] not in ["", None]:
                field_type = 'TEXT'

            sql = "alter table %s add column %s %s" % (
                self.videlLayerName,
                field_name,
                field_type
            )

            # self.__addAlias(self.videlLayerName, field_name, field_desc[u"name"])
            self.__aliases.append([self.videlLayerName, field_name, field_desc[u"name"]])
            cur.execute(sql)
            export_fields.append(field_name)

        conn.commit()

        videl_count = dbf_table._count_records()
        videl_index = 1
        for row in dbf_table:
            
            if self.__interupt.isSet():
                raise ConverterInteruptException("Interupt")

            nomkvr = row.get(u"nomkvr", None)
            nomvyd = row.get(u"nomvyd", None)
            
            if nomkvr is None or nomvyd is None:
                continue
            
            values = [(v[0], self.__getDBFValueByField(v)) for v in row.items() if v[0] in export_fields]

            try:
                sql = "update %s set %s where nomkvr = %s and nomvyd = %s" % (
                    self.videlLayerName,
                    ", ".join(["%s=?" % v[0] for v in values]),
                    nomkvr,
                    nomvyd   
                )

                cur.execute(sql, [v[1] for v in values])

                self.__setStatusMessage("Process %d from %d videls" % (videl_index, videl_count))

                videl_index += 1
            except Exception as err:
                self.__exceptions.append("Update %s where nomkvr = %s and nomvyd = %s error: %s" % (
                    self.videlLayerName,
                    nomkvr,
                    nomvyd,
                    str(err)
                ))

        conn.commit()

    def __processPhl2(self):
        phl2_dbf_file = self.__lfs.getPHLDataFiles()[0]['phl2']

        dbf_table = DBF(
            phl2_dbf_file,
            lowernames=True,
            encoding=LESIS_ENCODING
        )

        fields = []
        for export_field in dbf_table.fields:

            field_name = export_field.name
            field_type = typemap.get(export_field.type, 'TEXT')

            field_desc = self.__lessis_fields_struct.get(field_name, None)
            if field_desc is None:
                field_type = 'TEXT'
            elif field_desc[u"ref"] not in ["", None]:
                field_type = 'TEXT'

            fields.append( (field_name, field_type) )

            if field_desc is not None:
                # self.__addAlias(self.yarporLayerName, field_name, field_desc[u"name"])
                self.__aliases.append([self.yarporLayerName, field_name, field_desc[u"name"]])
            else:
                self.__exceptions.append(u"Field %s not present in Fields.DBF" % field_name)

        conn = sqlite3.connect(self.__sqlite_filename)
        cur = conn.cursor()

        cur.execute("PRAGMA foreign_keys = ON")
        conn.commit()

        sql = "CREATE TABLE %s (%s, %s)" % (
            self.yarporLayerName,
            ", ".join( ["%s %s" % field for field in fields] ),
            "videl_id INTEGER REFERENCES %s(ogc_fid)" % (self.videlLayerName,)
        )
        cur.execute(sql)
        conn.commit()
        self.__aliases.append([self.yarporLayerName, None, u"Ярусы"])

        videls = self.__getVidels(cur)

        yar_count = dbf_table._count_records()
        yar_index = 1
        for row in dbf_table:
            
            if self.__interupt.isSet():
                raise ConverterInteruptException("Interupt")

            nnn = unicode(row.get(u"nnn", None))
            fields = [(v[0], self.__getDBFValueByField(v)) for v in row.items()]        

            try:
                sql = "insert into %s (%s, videl_id) values (%s, ?)" % (
                    self.yarporLayerName,
                    ", ".join([field[0] for field in fields]),
                    ", ".join(["?" for field in fields]),
                )

                values = [field[1] for field in fields]
                values.append(videls.get(nnn))

                cur.execute(sql, values)

                self.__setStatusMessage("Process %d from %d yaruses" % (yar_index, yar_count))

                yar_index += 1
            except Exception as err:
                self.__exceptions.append("Insert into %s error: %s" % (
                    self.yarporLayerName,
                    str(err)
                ))

        conn.commit()

    def __getVidels(self, cur):
        sql = "select ogc_fid, nnn from %s" % (
            self.videlLayerName,
        )
        cur.execute(sql)
        videls_list = cur.fetchall()
        videls = {}
        for videl in videls_list:
            videls.update({videl[1]: videl[0]})

        return videls

    def __processPhl3(self):
        phl3_dbf_file = self.__lfs.getPHLDataFiles()[0].get('phl3')
        if phl3_dbf_file is None:
            self.__exceptions.append("There is not phl3.dbf")
            return

        dbf_table = DBF(
            phl3_dbf_file,
            lowernames=True,
            encoding=LESIS_ENCODING
        )

        fields = {}
        for field in dbf_table.fields:
            fields[field.name] = field.type

        makets_ids = set()
        for row in dbf_table:
            makets_ids.add(row.get(u"maket", None))
                
        referenced_fields =  self.__createMaketsTables(makets_ids, fields)
        conn = sqlite3.connect(self.__sqlite_filename)
        cur = conn.cursor()

        videls = self.__getVidels(cur)

        maket_count = dbf_table._count_records()
        maket_index = 1
        for row in dbf_table:
            if self.__interupt.isSet():
                raise ConverterInteruptException("Interupt")

            maket_id = row.get(u"maket", None)
            if maket_id is None:
                continue

            nnn = unicode(row.get(u"nnn", None))
            videl_fid = videls.get(nnn)

            if videl_fid is None:
                self.__exceptions.append(u"Skip maket - there is no videl with nnn: " + nnn)
                continue

            fields = [field[0] for field in row.items()]
            values = [field[1] for field in row.items()]

            for field_index in xrange(0, len(fields)):
                reference_dbf_name = referenced_fields.get((maket_id, fields[field_index]))
                if reference_dbf_name is None:
                    continue
                reference_dbf = self.__lfs.getDBFbyName(reference_dbf_name)
                if reference_dbf_name is None:
                    continue

                ref_value = self.__getDBFValueByDBF((reference_dbf, values[field_index]))
                values[field_index] = ref_value

            fields.append("videl_id")
            values.append(videl_fid)

            maket_table_name = self.maket_table_name_pattern % maket_id
            sql = "insert into %s (%s) values (%s)" % (
                maket_table_name,
                ", ".join([field for field in fields]),
                ", ".join(["?" for field in fields]),
            )
            cur.execute(sql, values)

            self.__setStatusMessage("Process %d from %d makets" % (maket_index, maket_count))
            maket_index += 1

        conn.commit()

    def __createMaketsTables(self, makets_ids, fields):
        makets_dbf = self.__lfs.getDBFbyName(u"makets")

        if makets_dbf is None:
            self.__exceptions.append("Makets.bdf not found! Makets tables will not be created.")
            return False

        dbf_table = DBF(
            makets_dbf,
            lowernames=True,
            encoding=LESIS_ENCODING
        )

        makets_tables_struct = {}
        for row in dbf_table:
            maket_id = row.get(u"maket")
            if maket_id not in makets_ids:
                continue

            field = unicode(row.get(u"field"))
            if field == u"N0":
                makets_tables_struct[maket_id] = []

                alias = row.get(u"name")
                maket_table_name = self.maket_table_name_pattern % unicode(maket_id)
                # self.__addAlias(maket_table_name, None, alias)
                self.__aliases.append([maket_table_name, None, alias])

        referenced_fields = {}
        for row in dbf_table:
            maket_id = row.get(u"maket")            
            if maket_id not in makets_ids:
                continue

            field = unicode(row.get(u"field")).lower()
            if field != u"n0":
                if makets_tables_struct.has_key(maket_id):
                    
                    field_type = typemap.get(
                        fields.get(field),
                        'TEXT'
                    )
                    sprav = unicode(row.get(u"sprav"))
                    if sprav not in ["", None]:
                        field_type = 'TEXT'
                        referenced_fields[(maket_id, field)] = sprav

                    makets_tables_struct[maket_id].append((field, field_type))

                    alias = row.get(u"name")
                    maket_table_name = self.maket_table_name_pattern % unicode(maket_id)
                    # self.__addAlias(maket_table_name, field, alias)
                    self.__aliases.append([maket_table_name, field, alias])
                else:
                    self.__exceptions.append(u"Makets %s not found!" % maket_id)
        
        for maket_id, maket_fields in makets_tables_struct.items():
            if self.__interupt.isSet():
                raise ConverterInteruptException("Interupt")

            maket_fields_ids = [field[0] for field in maket_fields]
            miss_fields = [(field[0], typemap.get(field[1], 'TEXT')) for field in fields.items() if field[0] not in maket_fields_ids]
            maket_fields.extend(miss_fields)
            self.__createMaketTable(maket_id, maket_fields)

        return referenced_fields

    def __createMaketTable(self, maket_id, fields):
        maket_table_name = self.maket_table_name_pattern % maket_id
        conn = sqlite3.connect(self.__sqlite_filename)
        cur = conn.cursor()

        cur.execute("PRAGMA foreign_keys = ON")
        conn.commit()

        sql = "CREATE TABLE %s (%s, %s)" % (
            maket_table_name,
            ", ".join( ["%s %s" % field for field in fields] ),
            "videl_id INTEGER REFERENCES %s(ogc_fid)" % (self.videlLayerName,)
        )

        cur.execute(sql)
        conn.commit()

    def __createAliasesTable(self):
        conn = sqlite3.connect(self.__sqlite_filename)
        cur = conn.cursor()

        sql = "CREATE TABLE %s (table_name TEXT, field_name TEXT, alias TEXT)" % (
            self.alias_table_name
        )

        cur.execute(sql)
        conn.commit()

        for alias in self.__aliases:
            if self.__interupt.isSet():
                raise ConverterInteruptException("Interupt")

            sql = "insert into %s (%s) values (%s)" % (
                self.alias_table_name,
                "table_name, field_name, alias",
                "?, ?, ?",
            )
            cur.execute(sql, alias)
        conn.commit()            

    # def __addAlias(self, table_name, field_name, alias):
    #     conn = sqlite3.connect(self.__sqlite_filename)
    #     cur = conn.cursor()

    #     sql = "insert into %s (%s) values (%s)" % (
    #         self.alias_table_name,
    #         "table_name, field_name, alias",
    #         "?, ?, ?",
    #     )
    #     cur.execute(sql, [table_name, field_name, alias])
    #     conn.commit()


def main():
    sqliteDBDest = u"e:\\dev\\test.lesis\\new_lesis.sqlite"
    shapeFile = u"e:\\dev\\test.lesis\\VD\\Выдел.SHP"
    lBaseDir = u"e:\\dev\\test.lesis\\rlh"
 
    def myprint(msg):
        print msg

    import signal

    cnvr = Converter(lBaseDir, shapeFile, sqliteDBDest)
    cnvr.setStatusChangedCallback(myprint)

    signal.signal( signal.SIGINT, lambda s, f : cnvr.interupt())

    cnvr.convert()

    print "\nExceptions:\n" + "\n".join(cnvr.getExceptions())

if __name__ == '__main__':
    main()
