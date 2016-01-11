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

from PyQt4 import QtCore
from PyQt4 import QtGui

from qgis_plugin_base import Plugin
from worker import Worker


class InputValue(QtCore.QObject):
    def __init__(self, vName, vType, defaultValue):
        QtCore.QObject.__init__(self)

        self._name = vName
        self._type = vType
        self._defaultValue = defaultValue
        self._settingsKey = "%s/%s" % (Plugin().getPluginName(), self._name)

    def getValue(self):
        value =  QtCore.QSettings().value(
            self._settingsKey,
            self._defaultValue, 
            type=self._type)

        return value

    def setValue(self, value):
        QtCore.QSettings().setValue(self._settingsKey, value)


class IVFile(QtGui.QWidget, InputValue):
    def __init__(self, name, title, chooserCaption, parent=None):
        InputValue.__init__(self, name, unicode, u"")
        QtGui.QWidget.__init__(self, parent)

        self._chooserCaption = chooserCaption

        self._layout = QtGui.QHBoxLayout(self)

        self.__lable = QtGui.QLabel(title)
        self.__lable.setFixedWidth(80)
        self.__lable.setWordWrap(True)
        self._layout.addWidget(self.__lable)
        
        self._inputValue = QtGui.QLineEdit(self.getValue())
        self._inputValue.editingFinished.connect(self.saveValue)
        self._layout.addWidget(self._inputValue)

        self.__button = QtGui.QPushButton(QtCore.QCoreApplication.translate("lesis2sqlite", "Browse"))
        self.__button.clicked.connect(self.chooseFile)
        self._layout.addWidget(self.__button)        

    def chooseFile(self):
        chooserDir = QtCore.QDir.homePath()
        v = self.getValue()
        if (v != u"") and os.path.exists(v):
            chooserDir = os.path.dirname(self.getValue())

        fName = QtGui.QFileDialog.getOpenFileName(self, self._chooserCaption, chooserDir)

        self._inputValue.setText(fName)
        self.saveValue()

    def saveValue(self):
        v = self._inputValue.text()
        self.setValue(v)


class IVNewFile(IVFile):
    def chooseFile(self):
        chooserDir = QtCore.QDir.homePath()
        v = self.getValue()
        if (v != u"") and os.path.exists(v):
            chooserDir = os.path.dirname(self.getValue())

        fName = QtGui.QFileDialog.getSaveFileName(self, self._chooserCaption, chooserDir)

        self._inputValue.setText(fName)
        self.saveValue()


class IVDir(IVFile):
    def chooseFile(self):
        chooserDir = QtCore.QDir.homePath()
        v = self.getValue()
        if (v != u"") and os.path.exists(v):
            chooserDir = os.path.dirname(self.getValue())

        fName = QtGui.QFileDialog.getExistingDirectory(self, self._chooserCaption, chooserDir)

        self._inputValue.setText(fName)
        self.saveValue()


class Lesis2SQLiteDialog(QtGui.QDialog):
    layerSrcCreated = QtCore.pyqtSignal(unicode)

    def __init__(self, parent=None):
        QtGui.QDialog.__init__(self, parent)

        self.setWindowTitle(Plugin().getPluginName())
        
        self.__mainLayout = QtGui.QVBoxLayout(self)
        
        self.__layout = QtGui.QGridLayout(self)
        self.__mainLayout.addLayout(self.__layout)

        self.__layout.addWidget(QtGui.QLabel(self.tr("Lesis base dir") + ":"), 0, 0)
        self.__layout.addWidget(QtGui.QLabel(self.tr("Videl shape") + ":"), 1, 0)
        self.__layout.addWidget(QtGui.QLabel(self.tr("SQLite db") + ":"), 2, 0)

        settings = QtCore.QSettings()
        self.lesisBaseDir = QtGui.QLineEdit(self)
        self.lesisBaseDir.setText(
            settings.value("%s/lesisBaseDir" % (Plugin().getPluginName(), ), u"", type = unicode)
        )
        self.shape = QtGui.QLineEdit(self)
        self.shape.setText(
            settings.value("%s/shape" % (Plugin().getPluginName(), ), u"", type = unicode)
        )
        self.sqliteDB = QtGui.QLineEdit(self)
        self.sqliteDB.setText(
            settings.value("%s/sqliteDB" % (Plugin().getPluginName(), ), u"", type = unicode)
        )
        self.__layout.addWidget(self.lesisBaseDir, 0, 1)
        self.__layout.addWidget(self.shape, 1, 1)
        self.__layout.addWidget(self.sqliteDB, 2, 1)

        self.btnLesisBaseDir = QtGui.QPushButton(self.tr("Browse"), self)
        self.btnLesisBaseDir.clicked.connect(self.chooseLesisBaseDir)
        self.btnShape = QtGui.QPushButton(self.tr("Browse"), self)
        self.btnShape.clicked.connect(self.chooseShapeFile)
        self.btnSQLiteDB = QtGui.QPushButton(self.tr("Browse"), self)
        self.btnSQLiteDB.clicked.connect(self.choosesqliteDB)

        self.__layout.addWidget(self.btnLesisBaseDir, 0, 2)
        self.__layout.addWidget(self.btnShape, 1, 2)
        self.__layout.addWidget(self.btnSQLiteDB, 2, 2)
        
        # self.lesisBaseDir = IVDir("lesis_base_dir", self.tr("Lesis base dir"), self.tr("Select Lesis base dir"), self)
        # self.__layout.addWidget(self.lesisBaseDir)
        
        # self.shape = IVFile("lesis_shape", self.tr("Videls shape 1"), self.tr("Select Videls shape"), self)
        # self.__layout.addWidget (self.shape)

        # self.sqliteDB = IVNewFile("sqlite_db", self.tr("SQLite db 1"), self.tr("Select output sqlite DB"), self)
        # self.__layout.addWidget(self.sqliteDB)

        self.__mainLayout.addItem(QtGui.QSpacerItem(40, 20, QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Expanding))

        self.__statusLable =  QtGui.QLabel(self)
        self.__mainLayout.addWidget(self.__statusLable)
        
        self.__bbox = QtGui.QDialogButtonBox(QtGui.QDialogButtonBox.Ok)
        self.__bbox.setSizePolicy(QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Fixed)
        self.__bbox.accepted.connect(self.accept)
        self.__mainLayout.addWidget(self.__bbox)

        self.worker = None
        self.thread = None

    def chooseShapeFile(self):
        chooserDir = QtCore.QDir.homePath()
        
        currentFilename = self.shape.text()
        if os.path.exists(currentFilename):
            chooserDir = os.path.dirname(currentFilename)

        fName = QtGui.QFileDialog.getOpenFileName(self, self.tr("Select Videls shape"), chooserDir)

        if fName != u"":
            self.shape.setText(fName)

    def chooseLesisBaseDir(self):
        chooserDir = QtCore.QDir.homePath()
        
        currentDirname = self.lesisBaseDir.text()
        if os.path.exists(currentDirname):
            chooserDir = os.path.dirname(currentDirname)

        fName = QtGui.QFileDialog.getExistingDirectory(self, self.tr("Select Lesis base dir"), chooserDir)

        if fName != u"":
            self.lesisBaseDir.setText(fName)

    def choosesqliteDB(self):
        chooserDir = QtCore.QDir.homePath()
        
        currentFilename = self.sqliteDB.text()
        if os.path.exists(currentFilename):
            chooserDir = os.path.dirname(currentFilename)

        fName = QtGui.QFileDialog.getSaveFileName(self, self.tr("Select output sqlite DB"), chooserDir)

        if fName != u"":
            self.sqliteDB.setText(fName)

    def validate(self):
        exceptions = []
        if not os.path.exists(self.lesisBaseDir.text()):
            exceptions.append(self.tr("Specified Lesis base dir not found!"))

        if not os.path.exists(self.shape.text()):
            exceptions.append(self.tr("Specified shape file not found!"))

        if self.sqliteDB.text() == u"":
            exceptions.append(self.tr("Sqlite db file not specified!"))

        if len(exceptions) > 0:
            QtGui.QMessageBox.critical(self, self.tr("Validate error"), "\n".join(exceptions))
            return False

        return True

    def accept(self):
        
        if not self.validate():
            return

        settings = QtCore.QSettings()
        settings.setValue("%s/lesisBaseDir" % (Plugin().getPluginName(), ), self.lesisBaseDir.text())
        settings.setValue("%s/shape" % (Plugin().getPluginName(), ), self.shape.text())
        settings.setValue("%s/sqliteDB" % (Plugin().getPluginName(), ), self.sqliteDB.text())

        self.__bbox.button(QtGui.QDialogButtonBox.Ok).setEnabled(False)

        worker = Worker(
            self.lesisBaseDir.text(),
            self.shape.text(),
            self.sqliteDB.text()
        )
        thread = QtCore.QThread(self)

        worker.moveToThread(thread)
        worker.statusChanged.connect(self.changeProgressStatus)
        worker.progressChanged.connect(self.changeProgress)
        worker.error.connect(self.workerErorrProcess)

        thread.started.connect(worker.run)
        worker.stoped.connect(self.addLayer)
        worker.stoped.connect(thread.quit)
        worker.stoped.connect(worker.deleteLater)
        worker.stoped.connect(thread.deleteLater)
        worker.stoped.connect(self.close)
        thread.start()

        self.thread = thread
        self.worker = worker

    def reject(self):
        Plugin().plPrint("reject")

        if self.worker is not None:
            self.worker.interupt()
        
        return QtGui.QDialog.reject(self)

    def workerErorrProcess(self, msg):
        QtGui.QMessageBox.critical(self, "Export error", msg)

    def changeProgressStatus(self, status):
        self.__statusLable.setText(status)

    def addLayer(self):
        self.layerSrcCreated.emit(self.sqliteDB.text())
