# -*- coding: utf-8 -*-
#******************************************************************************
#
# lesis2sqlite
# ---------------------------------------------------------
# This plugin convert lesis GIS working dir structure to sqlite data base
#
# Author:   Alexander Lisovenko, alexander.lisovenko@nextgis.ru
# *****************************************************************************
# Copyright (c) 2015-2016. NextGIS, info@nextgis.com
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

from PyQt4 import QtCore

from qgis import gui, core

from qgis_plugin_base import Plugin

from lesisqlite import lesis2sqlite


class Worker(QtCore.QObject):

    statusChanged = QtCore.pyqtSignal(unicode)
    progressChanged = QtCore.pyqtSignal(int)
    stoped = QtCore.pyqtSignal()
    error = QtCore.pyqtSignal(unicode)

    def __init__(self, lesisBaseDir, lesisVidelsShape, sqliteDB):
        QtCore.QObject.__init__(self)

        Plugin().plPrint("Worker __init__")

        self.sqliteDBDest = sqliteDB
        self.shapeFile = lesisVidelsShape
        self.lBaseDir = lesisBaseDir

        self.cnvr = lesis2sqlite.Converter(
            self.lBaseDir,
            self.shapeFile,
            self.sqliteDBDest
        )
        self.cnvr.setStatusChangedCallback(self.statusChanged.emit)
        self.cnvr.setSetCRSCallback(self.getCRSAsWKT)

    def interupt(self):
        self.cnvr.interupt()

    def run(self):
        try:
            self.cnvr.convert()

            # self.statusChanged.emit(u"Analise directories structure")

            # lfs = lesis2sqlite.LesisFileStructure(self.lBaseDir)

            # fieldsDesc = lesis2sqlite.getFieldsDescFromDBF(lfs.getFieldsDBFFile())
            # fieldsDescExt = {
            #     u"arn": {
            #         "type": u"char",
            #         "name": u"Арендатор",
            #         "ref":  u"arnLesse",
            #     },
            #     u"vozrub": {
            #         "type": u"char",
            #         "name": u"Возраст рубки",
            #         "ref":  None,
            #     }
            # }
            # fieldsDesc.update(fieldsDescExt)
            # data_files = lfs.getPHLDataFiles()[0]
            
            # self.statusChanged.emit(u"Copy shape")
            # layerName = lesis2sqlite.shape2sqlite(
            #     self.shapeFile,
            #     self.sqliteDBDest,
            #     self.getCRSAsWKT,
            #     self.__copyFeatureCallback)
            
            # shapeDS = self.__getShapeDS()
            # sqliteDS = self.__getSQLiteDS()
            # self.__shape2sqlite(shapeDS, sqliteDS)
            # shapeDS = None
            # sqliteDS = None
            
            #self.statusChanged.emit(u"Process PHL1 dbf", 0, videl_count)
            # exceptions = lesis2sqlite.processPHL1(
            #     data_files["phl1"],
            #     layerName,
            #     self.sqliteDBDest,
            #     fieldsDesc,
            #     lfs,
            #     self.__processPhl1Feature
            # )

            # Plugin().plPrint("exceptions: " + str(exceptions))
            
            # self.statusChanged.emit(u"Process PHL2 dbf")
            # lesis2sqlite.processPHL2(data_files["phl2"], "yarpor_attrs", self.sqliteDBDest, fieldsDesc)

            # self.statusChanged.emit(u"Add kvartales")
            # lesis2sqlite.addKVRpoligonLayer(layerName, self.sqliteDBDest)

            # self.statusChanged.emit(u"Finish")

        except Exception as err:
            self.error.emit(str(err))

        
        
        Plugin().plPrint("Exceptions:\n" + "\n".join(self.cnvr.getExceptions()))
        
        self.stoped.emit()
        Plugin().plPrint("Finish")

    def getCRSAsWKT(self):
        selector = gui.QgsGenericProjectionSelector()
        selector.exec_()

        # crs = core.QgsCoordinateReferenceSystem(selector.selectedCrsId())
        crs = core.QgsCoordinateReferenceSystem(selector.selectedAuthId())
        Plugin().plPrint('crs.toWkt(): ' + crs.toWkt())
        return crs.toWkt()

    def __copyFeatureCallback(self, fetureIndex, featureCount):
        msg = "Copy feature %d from %d" % (fetureIndex, featureCount)
        self.statusChanged.emit(msg)

    def __processPhl1Feature(self, fetureIndex, featureCount):
        msg = "Process phl1 %d from %d" % (fetureIndex, featureCount)
        Plugin().plPrint(msg)
        self.statusChanged.emit(msg)

    # def __shape2sqlite(self, shapeDS, sqliteDS):
    #     layerSrc = shapeDS.GetLayerByIndex(0)

    #     featureCount = layerSrc.GetFeatureCount()
    #     # featureCount = 100
    #     self.statusChanged.emit(u"Convert videls shape to sqlite", 0, featureCount, u"step")

    #     crs = layerSrc.GetSpatialRef()
    #     if crs is None:
    #         crs = osr.SpatialReference()
    #         crs.ImportFromWkt(self.getCRSAsWKT())

    #     #sqliteDS.CopyLayer(layerSrc, self.videlLyerName, ["OVERWRITE=YES"])
    #     layerDst = sqliteDS.CreateLayer(
    #         str(self.videlLayerName),
    #         crs,
    #         layerSrc.GetGeomType(),
    #         ["OVERWRITE=YES"]
    #     )

    #     # adding fields to new layer
    #     layer_definition = ogr.Feature(layerSrc.GetLayerDefn())
    #     for i in range(layer_definition.GetFieldCount()):
    #         layerDst.CreateField(layer_definition.GetFieldDefnRef(i))

    #     # adding the features from input to dest
    #     for i in range(0, featureCount):
    #         feature = layerSrc.GetFeature(i)
    #         layerDst.CreateFeature(feature)

    #         self.progressChanged.emit(i)
    #         layerDst.SyncToDisk()
            
    # def __getShapeDS(self):
    #     shapeDS = ogr.Open(self.shapeFile)
    
    #     if shapeDS is None:
    #         return None

    #     if shapeDS.GetDriver().GetName() != "ESRI Shapefile":
    #         return None

    #     return shapeDS

    # def __getSQLiteDS(self):
    #     if os.path.isfile(self.sqliteDBDest):
    #         sqliteDS = ogr.Open(self.sqliteDBDest, True)
            
    #         if sqliteDS is None:
    #             return None

    #         if sqliteDS.GetDriver().GetName() != "SQLite":
    #             return None
    #     else:
    #         drv = ogr.GetDriverByName("SQLite")
    #         sqliteDS = drv.CreateDataSource(self.sqliteDBDest)

    #         if sqliteDS is None:
    #             return None

    #     return sqliteDS

    # def processPHL1(self, tbl_name, sqlite_db_file, fields_desc, lesis_file_structure):
    #     def getDBFValue(v):
    #         '''
    #             dbf_file_name
    #             dbf_file_value
    #         '''
    #         dbf_file_name = v[0]
    #         dbf_file_value = unicode(v[1])

    #         # print "getDBFValue((%s, %s))" %(dbf_file_name, dbf_file_value)

    #         field_desc = fields_desc.get(dbf_file_name)

    #         if field_desc[u"ref"] in ["", None]:
    #             return v

    #         # print "    reftype! ref: ", field_desc[u"ref"]
    #         ref_dbf_file = lesis_file_structure.getDBFbyName(field_desc[u"ref"])
    #         if ref_dbf_file is None:
    #             # print "    Reference file not found!"
    #             return (dbf_file_name, u"Reference file not found!")

    #         # print "    ref_dbf_file: ", ref_dbf_file

    #         cashed_value = reference_cash.get((ref_dbf_file, dbf_file_value), None)
    #         if cashed_value is not None:
    #             # print "    cashed: ", cashed_value
    #             return (dbf_file_name, cashed_value)

    #         # print "Not in cach"

    #         ref_dbf_table = DBF(
    #             ref_dbf_file,
    #             lowernames=True,
    #             encoding=LESIS_ENCODING
    #         )
            
    #         kl_type = u"N"
    #         for dbf_field in ref_dbf_table.fields:
    #             if dbf_field.name == u"kl":
    #                 kl_type = dbf_field.type
    #         # print "    kl_type: ", kl_type

    #         is_found = False
    #         for row in ref_dbf_table:
    #             # TODO get find KL and TX field exception
    #             kl = row.get(u"kl")
    #             if kl_type in [u"I", u"F", u"N", u"0"] and kl is None:
    #                 kl = 0
    #             kl = unicode(kl)
    #             tx = row.get(u"tx", None)

    #             if dbf_file_value == kl:
    #                 is_found = True
    #                 kl = dbf_file_value

    #             reference_cash.update({(ref_dbf_file, kl): tx})

    #             # if dbf_file_name in [u"tipvyr"]:
    #             #     print "        add to cache: ", {(ref_dbf_file, kl): tx} 
                    
    #         if not is_found:
    #             # print "    add to cash as None!"
    #             reference_cash.update({(ref_dbf_file, dbf_file_value): None})

    #         # print "    return: ", reference_cash[(ref_dbf_file, dbf_file_value)]  
    #         return (dbf_file_name, reference_cash[(ref_dbf_file, dbf_file_value)] )

    #     dbf_table = DBF(
    #         self.lfs.getPHLDataFiles()[0][u"phl1"],
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
    #     videl_index = 1

    #     for row in dbf_table:
    #         nomkvr = row.get(u"nomkvr", None)
    #         nomvyd = row.get(u"nomvyd", None)
            
    #         if nomkvr is None or nomvyd is None:
    #             continue

    #         values = [getDBFValue(v) for v in row.items() if v[0] in export_fields]

    #         # print values

    #         try:
    #             sql = "update %s set %s where nomkvr = %s and nomvyd = %s" % (
    #                 tbl_name,
    #                 ", ".join(["%s=?" % v[0] for v in values]),
    #                 nomkvr,
    #                 nomvyd   
    #             )
    #             # print sql

    #             cur.execute(sql, [v[1] for v in values])
    #             subscriber_func(videl_index)
    #             videl_index += 1
    #         except Exception as err:
    #             exceptions.append("Update %s where nomkvr = %s and nomvyd = %s error: %s" % (
    #                 tbl_name,
    #                 nomkvr,
    #                 nomvyd,
    #                 str(err)
    #             ))

    #         # break

    #     conn.commit()
    #     return exceptions
