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

reference_cash = {}

def videlsCount(sqlite_db_file, tbl_name):
    conn = sqlite3.connect(sqlite_db_file)
    cur = conn.cursor()

    cur.execute("select count(*) from %s" % tbl_name)
    videl_count = cur.fetchone()[0]
    return videl_count

def processPHL1(phl1_dbf_file, tbl_name, sqlite_db_file, fields_desc, lesis_file_structure, subscriber_func = None):
    
    def getDBFValue(v):
        '''
            dbf_file_name
            dbf_file_value
        '''
        dbf_file_name = v[0]
        dbf_file_value = unicode(v[1])

        field_desc = fields_desc.get(dbf_file_name)

        if field_desc[u"ref"] in ["", None]:
            return v[1] 

        ref_dbf_file = lesis_file_structure.getDBFbyName(field_desc[u"ref"])
        if ref_dbf_file is None:
            return u"Reference file not found!"

        try:
            return reference_cash[(ref_dbf_file, dbf_file_value)]

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

                if dbf_file_value == kl:
                    is_found = True
                    kl = dbf_file_value

                reference_cash.update({(ref_dbf_file, kl): tx})

            if not is_found:
                reference_cash.update({(ref_dbf_file, dbf_file_value): None})

            return (dbf_file_name, reference_cash[(ref_dbf_file, dbf_file_value)] )

    dbf_table = DBF(
        phl1_dbf_file,
        lowernames=True,
        encoding=LESIS_ENCODING
    )

    conn = sqlite3.connect(sqlite_db_file)
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(%s)" % tbl_name)
    existing_fileds = [field[1] for field in cur.fetchall()]

    export_fields = []
    for export_field in dbf_table.fields:
        field_name = export_field.name
        field_type = typemap.get(export_field.type, 'TEXT')

        if field_name in existing_fileds:
            continue

        field_desc = fields_desc.get(export_field.name, None)
        if field_desc is None:
            continue

        if field_desc[u"ref"] not in ["", None]:
            field_type = 'TEXT'

        sql = "alter table %s add column %s %s" % (tbl_name, field_name, field_type)
        cur.execute(sql)
        export_fields.append(field_name)

    conn.commit()

    exceptions = []

    videl_count = dbf_table._count_records()
    videl_index = 1
    for row in dbf_table:
        
        nomkvr = row.get(u"nomkvr", None)
        nomvyd = row.get(u"nomvyd", None)
        
        if nomkvr is None or nomvyd is None:
            continue
        
        values = [(v[0], getDBFValue(v)) for v in row.items() if v[0] in export_fields]

        try:
            sql = "update %s set %s where nomkvr = %s and nomvyd = %s" % (
                tbl_name,
                ", ".join(["%s=?" % v[0] for v in values]),
                nomkvr,
                nomvyd   
            )

            cur.execute(sql, [v[1] for v in values])
            subscriber_func(videl_index, videl_count)
            videl_index += 1
        except Exception as err:
            exceptions.append("Update %s where nomkvr = %s and nomvyd = %s error: %s" % (
                tbl_name,
                nomkvr,
                nomvyd,
                str(err)
            ))

        #print("----- %s seconds ---" % (time.time() - start_time_1))

    conn.commit()
    return exceptions

def processPHL2(phl1_dbf_file, layer_name, sqlite_db_file, fields_desc):
    pass


def addKVRpoligonLayer(layer_name, sqlite_db_file, kvr_plg_layer_name="kvr_plg"):
    conn = sqlite3.connect(sqlite_db_file)
    cur = conn.cursor()

    sql = "select DISTINCT nomkvr from %s" % layer_name
    cur.execute(sql)
    
    kvrs_info = cur.fetchall()

    cur.close()
    conn.close()

    create_new_plg_layer(
        layer_name,
        sqlite_db_file,
        kvr_plg_layer_name,
        [kvr_info[0] for kvr_info in kvrs_info]
    )


def parse_args():
    import argparse
    parser = argparse.ArgumentParser(
        description='usage: %prog lesis_export_base_dir')
    arg = parser.add_argument

    arg('--lesis_export_base_dir',
        action='store',
        dest='lesis_export_base_dir',
        default=None,
        help='lesis export data base dir ')

    return parser.parse_args()

class ConverterInteruptException(Exception):
    def __init__(self, message):
        super(ConverterInteruptException, self).__init__(message)

class ConverterErrorException(Exception):
    def __init__(self, message):
        super(ConverterErrorException, self).__init__(message)

class Converter(object):
    RETURN_CODE_SUCCESS = 0
    RETURN_CODE_ERROR = 1
    RETURN_CODE_INTERUPT = 2

    def __init__(self, lesis_base_dir, shape_filename, sqlite_filename):

        self.__lesis_base_dir   = lesis_base_dir
        self.__shape_filename   = shape_filename
        self.__sqlite_filename  = sqlite_filename

        self.__lfs = None
        self.__lessis_fields_struct = {}
        self.__reference_cash = {}
        self.__exceptions = []

        self.videlLayerName = "videl_plg"
        self.kvrLayerName = "kvr_plg"
        self.yarporLayerName = "yarpor_attr"

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

    def convert(self):
        self.__setStatus("Convert start")
        
        try:
            if os.path.exists(self.__sqlite_filename):
                os.remove(self.__sqlite_filename)

            self.__validateLesisWorkDir()
            
            self.__copyShape()
            
            self.__createKvrLayer()

            self.__processPhl1()

            self.__processPhl2()

            return self.RETURN_CODE_SUCCESS
        except ConverterErrorException as err:
            self.__exceptions.append(err.message)
            self.__setStatus(err.message)
            return self.RETURN_CODE_ERROR            

        except ConverterInteruptException:
            return self.RETURN_CODE_INTERUPT

    def __setStatus(self, status_msg):
        if self.__status_changed_callback is not None:
            self.__status_changed_callback(status_msg)

    def __validateLesisWorkDir(self):
        self.__setStatus("Analize lesis dir struct. Start")
        
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

            self.__setStatus("Analize lesis dir struct. Finish")

        except ValueError as err:
            self.__return_code = self.RETURN_CODE_ERROR

            msg = "Analize lesis dir struct. Error: " + err
            self.__exceptions.append(msg)
            self.__setStatus(msg)
            
    def __getShapeDS(self):
        shape_ds = ogr.Open(self.__shape_filename)
        procedure_name = "Crate shape ogr data source."
        if shape_ds is None:
            msg = procedure_name + " Error: " + "can't create ogr datasource for shape."
            raise ConverterErrorException(msg)

        if shape_ds.GetDriver().GetName() != "ESRI Shapefile":
            msg = procedure_name + " Error: " + "input file have not shape format."
            raise ConverterErrorException(msg)

        return shape_ds 

    def __getSQLiteDS(self):
        sqlite_ds = None
        procedure_name = "Crate sqlite ogr data source."
        
        drv = ogr.GetDriverByName("SQLite")
        # sqlite_ds = drv.CreateDataSource(self.__sqlite_filename, ["SPATIALITE=YES"])
        sqlite_ds = drv.CreateDataSource(self.__sqlite_filename, ["SPATIALITE=YES"])

        if sqlite_ds is None:
            msg = procedure_name + " Error: " + "can't create ogr datasource for sqlite."
            raise ConverterErrorException(msg)

        return sqlite_ds

    def __copyShape(self):
        procedure_name = "Copy shape file."
        self.__setStatus(procedure_name + " Start")

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
        # adding the features from input to dest
        feature_count = layer_src.GetFeatureCount()
        # feature_count = 10

        for i in range(0, feature_count):
            if self.__interupt.isSet():
                raise ConverterInteruptException(procedure_name + " Interupt")

            feature = layer_src.GetNextFeature()
            # feature = layer_src.GetFeature(i)
            layer_dst.CreateFeature(feature)
            
            self.__setStatus(procedure_name + " Copied %d from %d features" % (i, feature_count))

            time.sleep(0.01)
        layer_dst.SyncToDisk()

        dest_ds.Release()
        src_shape_ds.Release()

        src_shape_ds = None
        dest_ds = None

        self.__setStatus(procedure_name + " Finish")
        return layer_name

    def __createKvrLayer(self):
        procedure_name = "Create kvr layer."
        self.__setStatus(procedure_name + " Start")

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
            msg = procedure_name + " Error: " + "can't create ogr layer."
            raise ConverterErrorException(msg)

        field_defn = ogr.FieldDefn( "plskvr", ogr.OFTReal )
        if layer_dst.CreateField ( field_defn ) != 0:
            msg = procedure_name + " Error: " + "can't create ogr field 'plskvr'."
            raise ConverterErrorException(msg)

        nomkvrFieldDefn = featureDefn.GetFieldDefn(featureDefn.GetFieldIndex("nomkvr"))    
        if layer_dst.CreateField ( nomkvrFieldDefn ) != 0:
            msg = procedure_name + " Error: " + "can't create ogr field 'nomkvr'."
            raise ConverterErrorException(msg)

        for kvr_num in kvrs_nums:
            layer_src.SetAttributeFilter("nomkvr=%s" % str(kvr_num))

            result_polygon = ogr.Geometry(ogr.wkbPolygon)
            feature = layer_src.GetNextFeature()
            plskvr = feature.GetFieldAsDouble(feature.GetFieldIndex( "plsvyd" ))
            
            while feature is not None:
                if self.__interupt.isSet():
                    raise ConverterInteruptException(procedure_name + " Interupt")

                geom = feature.GetGeometryRef()
                union_geom = result_polygon.Union(geom)

                if union_geom is not None:
                    result_polygon = union_geom
                
                plskvr += feature.GetFieldAsDouble(feature.GetFieldIndex( "plsvyd" ))

                self.__setStatus(procedure_name + " Process videl %d" % (feature.GetFID()))

                feature = layer_src.GetNextFeature()
                time.sleep(0.01)

            feat = ogr.Feature( layer_dst.GetLayerDefn())
            feat.SetGeometry(result_polygon)
            feat.SetField( "plskvr", plskvr )
            feat.SetField( "nomkvr", str(kvr_num) )

            layer_dst.CreateFeature(feat)

        sqlite_ds.Release()
        sqlite_ds = None

        self.__setStatus(procedure_name + " Finish")

    def __getDBFValue(self, (field_name, field_value)):
        dbf_field_value = unicode(field_value)

        field_desc = self.__lessis_fields_struct.get(field_name)

        if field_desc is None:
            return None

        field_desc_ref = field_desc[u"ref"]
        
        if field_desc_ref in ["", None]:
            return field_value

        ref_dbf_file = self.__lfs.getDBFbyName(field_desc_ref)
        if ref_dbf_file is None:
            return u"Reference file not found!"

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
        procedure_name = "Process phl1 file."
        self.__setStatus(procedure_name + " Start")

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

            cur.execute(sql)
            export_fields.append(field_name)

        conn.commit()

        videl_count = dbf_table._count_records()
        videl_index = 1
        for row in dbf_table:
            
            if self.__interupt.isSet():
                raise ConverterInteruptException(procedure_name + " Interupt")

            nomkvr = row.get(u"nomkvr", None)
            nomvyd = row.get(u"nomvyd", None)
            
            if nomkvr is None or nomvyd is None:
                continue
            
            values = [(v[0], self.__getDBFValue(v)) for v in row.items() if v[0] in export_fields]

            try:
                sql = "update %s set %s where nomkvr = %s and nomvyd = %s" % (
                    self.videlLayerName,
                    ", ".join(["%s=?" % v[0] for v in values]),
                    nomkvr,
                    nomvyd   
                )

                cur.execute(sql, [v[1] for v in values])

                self.__setStatus(procedure_name + " Process %d from %d videls" % (videl_index, videl_count))

                videl_index += 1
            except Exception as err:
                self.__exceptions.append("Update %s where nomkvr = %s and nomvyd = %s error: %s" % (
                    self.videlLayerName,
                    nomkvr,
                    nomvyd,
                    str(err)
                ))

        conn.commit()
        self.__setStatus(procedure_name + " Finish")

    def __processPhl2(self):
        procedure_name = "Process phl2 file."
        self.__setStatus(procedure_name + " Start")

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

        sql = "select ogc_fid, nnn from %s" % (
            self.videlLayerName,
        )
        cur.execute(sql)
        videls_list = cur.fetchall()
        videls = {}
        for videl in videls_list:
            videls.update({videl[1]: videl[0]})

        yar_count = dbf_table._count_records()
        yar_index = 1
        for row in dbf_table:
            
            if self.__interupt.isSet():
                raise ConverterInteruptException(procedure_name + " Interupt")

            nnn = unicode(row.get(u"nnn", None))
            fields = [(v[0], self.__getDBFValue(v)) for v in row.items()]        

            try:
                sql = "insert into %s (%s, videl_id) values (%s, ?)" % (
                    self.yarporLayerName,
                    ", ".join([field[0] for field in fields]),
                    ", ".join(["?" for field in fields]),
                )

                values = [field[1] for field in fields]
                values.append(videls.get(nnn))

                cur.execute(sql, values)

                self.__setStatus(procedure_name + " Process %d from %d yaruses" % (yar_index, yar_count))

                yar_index += 1
            except Exception as err:
                self.__exceptions.append("Insert into %s error: %s" % (
                    self.yarporLayerName,
                    str(err)
                ))

        conn.commit()

        self.__setStatus(procedure_name + " Finish")


def main():
 
    # args = parse_args()

    # lesis_export = LesisExport(args.lesis_export_base_dir)
    # lesis_export.initExport = f1
    # lesis_export.dbfFileProcess = f2
    # lesis_export.handleError = lambda err: sys.stdout.write("!!!! " + str(err) + "\n")

    # # lesis_export.copy2SQLiteDB('d:\\Development\\NextGIS\\lesis2sqlite\\lesis_export.sqlite')
    # lesis_export.exportToSQLite('d:\\Development\\NextGIS\\lesis2sqlite\\lesis_export.sqlite')

    sqliteDBDest = u"e:\\dev\\lesis2sqlite\\new_lesis.sqlite"

    shapeFile = u"e:\\dev\\lesis2sqlite\\ПТЗ\\VD\\Выдел.SHP"
    # shapeFile = u"e:\dev\lesis2sqlite\VD\Videl.SHP"

    lBaseDir = u"e:\\dev\\lesis2sqlite\\ПТЗ\\rlh"
 
    lfs = LesisFileStructure(lBaseDir)

    print lfs.getDBFbyName("yarus")
    print lfs.getFieldsDBFFile()
    print lfs.getPHLDataFiles()

    fieldsDesc = getFieldsDescFromDBF(lfs.getFieldsDBFFile())
    fieldsDescExt = {
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
    fieldsDesc.update(fieldsDescExt)

    def myprint(msg):
        print msg

    # layer_name = shape2sqlite(shapeFile, sqliteDBDest, None, lambda x,y: myprint("%d:%d"%(x,y)))

    # print "layer_name: ", layer_name
    
    # data_files = lfs.getPHLDataFiles()[0]

    # processPHL1(data_files["phl1"], layer_name, sqliteDBDest, fieldsDesc, lfs)
    # processPHL2(data_files["phl2"], "yarpor_attrs", sqliteDBDest, fieldsDesc)

    # addKVRpoligonLayer(layer_name, sqliteDBDest)
    import signal

    cnvr = Converter(lBaseDir, shapeFile, sqliteDBDest)
    cnvr.setStatusChangedCallback(myprint)

    signal.signal( signal.SIGINT, lambda s, f : cnvr.interupt())

    cnvr.convert()

if __name__ == '__main__':
    main()
