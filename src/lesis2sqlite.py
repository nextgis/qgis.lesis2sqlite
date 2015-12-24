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
import sqlite3

from dbfread import DBF

import dbf2sqlite

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


def processPHL1(phl1_dbf_file, tbl_name, sqlite_db_file, fields_desc, lesis_file_structure):
    
    def getDBFValue(v):
        '''
            dbf_file_name
            dbf_file_value
        '''
        dbf_file_name = v[0]
        dbf_file_value = unicode(v[1])

        # print "getDBFValue((%s, %s))" %(dbf_file_name, dbf_file_value)

        field_desc = fields_desc.get(dbf_file_name)

        if field_desc[u"ref"] in ["", None]:
            return v

        # print "    reftype! ref: ", field_desc[u"ref"]
        ref_dbf_file = lesis_file_structure.getDBFbyName(field_desc[u"ref"])
        if ref_dbf_file is None:
            # print "    Reference file not found!"
            return (dbf_file_name, u"Reference file not found!")

        # print "    ref_dbf_file: ", ref_dbf_file

        cashed_value = reference_cash.get((ref_dbf_file, dbf_file_value), None)
        if cashed_value is not None:
            # print "    cashed: ", cashed_value
            return (dbf_file_name, cashed_value)

        # print "Not in cach"

        ref_dbf_table = DBF(
            ref_dbf_file,
            lowernames=True,
            encoding=LESIS_ENCODING
        )
        
        kl_type = u"N"
        for dbf_field in ref_dbf_table.fields:
            if dbf_field.name == u"kl":
                kl_type = dbf_field.type
        # print "    kl_type: ", kl_type

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

            # if dbf_file_name in [u"tipvyr"]:
            #     print "        add to cache: ", {(ref_dbf_file, kl): tx} 
                
        if not is_found:
            # print "    add to cash as None!"
            reference_cash.update({(ref_dbf_file, dbf_file_value): None})

        # print "    return: ", reference_cash[(ref_dbf_file, dbf_file_value)]  
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
    for row in dbf_table:
        nomkvr = row.get(u"nomkvr", None)
        nomvyd = row.get(u"nomvyd", None)
        
        if nomkvr is None or nomvyd is None:
            continue

        values = [getDBFValue(v) for v in row.items() if v[0] in export_fields]

        # print values

        try:
            sql = "update %s set %s where nomkvr = %s and nomvyd = %s" % (
                tbl_name,
                ", ".join(["%s=?" % v[0] for v in values]),
                nomkvr,
                nomvyd   
            )
            # print sql

            cur.execute(sql, [v[1] for v in values])

        except Exception as err:
            exceptions.append("Update %s where nomkvr = %s and nomvyd = %s error: %s" % (
                tbl_name,
                nomkvr,
                nomvyd,
                str(err)
            ))

        # break

    print "exceptions: ", exceptions

    conn.commit()

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


def f1(dbf_count):
    print "dbf_count: ", dbf_count


def f2(dbf_file):
    print "Process: ", dbf_file


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

    layer_name = shape2sqlite(shapeFile, sqliteDBDest)
    layer_name = "videl_plg"
    print "layer_name: ", layer_name
    
    data_files = lfs.getPHLDataFiles()[0]

    # processPHL1(data_files["phl1"], layer_name, sqliteDBDest, fieldsDesc, lfs)
    # processPHL2(data_files["phl2"], "yarpor_attrs", sqliteDBDest, fieldsDesc)

    addKVRpoligonLayer(layer_name, sqliteDBDest)

if __name__ == '__main__':
    main()
