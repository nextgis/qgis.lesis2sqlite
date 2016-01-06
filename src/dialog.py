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
    def __init__(self, name, chooserCaption, parent=None):
        InputValue.__init__(self, name, unicode, u"")
        QtGui.QWidget.__init__(self, parent)

        self._chooserCaption = chooserCaption

        self._layout = QtGui.QHBoxLayout(self)

        self.__lable = QtGui.QLabel(name + ":")
        self.__lable.setFixedWidth(80)
        self.__lable.setWordWrap(True)
        self._layout.addWidget(self.__lable)
        
        self._inputValue = QtGui.QLineEdit(self.getValue())
        self._inputValue.editingFinished.connect(self.saveValue)
        self._layout.addWidget(self._inputValue)

        self.__button = QtGui.QPushButton("Browse")
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

class Dialog(QtGui.QDialog):
    layerSrcCreated = QtCore.pyqtSignal(unicode)

    def __init__(self, parent=None):
        QtGui.QDialog.__init__(self, parent)

        self.setWindowTitle(Plugin().getPluginName())
        
        self.__layout = QtGui.QVBoxLayout(self)
        self.__layout.setSpacing(0)
        #self.__layout = QtGui.QGridLayout(self)

        # self.__layout.addWidget(QtGui.QLabel("Lesis base dir:"), 0, 0)
        # self.__layout.addWidget(QtGui.QLabel("Videl shape:"), 1, 0)

        # self.__layout.addWidget(QtGui.QLineEdit(), 0, 1)
        # self.__layout.addWidget(QtGui.QLineEdit(), 1, 1)

        # self.__layout.addWidget(QtGui.QPushButton("..."), 0, 2)
        # self.__layout.addWidget(QtGui.QPushButton("..."), 1, 2)
        
        self.lesisBaseDir = IVDir("Lesis base dir", "Select Lesis base dir", self)
        self.__layout.addWidget(self.lesisBaseDir)
        
        self.shape = IVFile("Videls shape", "Select Videls shape", self)
        self.__layout.addWidget (self.shape)

        self.sqliteDB = IVNewFile("SQLite db", "Select output sqlite DB", self)
        self.__layout.addWidget(self.sqliteDB)

        self.__layout.addItem(QtGui.QSpacerItem(40, 20, QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Expanding))

        self.__statusLable =  QtGui.QLabel(self)
        self.__layout.addWidget(self.__statusLable)
        # self.__pbar = QtGui.QProgressBar()
        # self.__pbar.setVisible(False)
        # self.__pbar.setTextVisible(True)
        # self.__layout.addWidget(self.__pbar)

        self.__bbox = QtGui.QDialogButtonBox(QtGui.QDialogButtonBox.Ok)
        self.__bbox.accepted.connect(self.accept)
        self.__layout.addWidget(self.__bbox)

        self.worker = None
        self.thread = None

    def accept(self):
        # self.__pbar.setVisible(True)

        self.__bbox.button(QtGui.QDialogButtonBox.Ok).setEnabled(False)

        worker = Worker(
            self.lesisBaseDir.getValue(),
            self.shape.getValue(),
            self.sqliteDB.getValue()
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

        # return QtGui.QDialog.accept(self)        

    def processWorkerLog(self, msg):
        #print "worker log: " + msg
        Plugin().plPrint("worker log: " + msg)

    def reject(self):
        Plugin().plPrint("reject")

        if self.worker is not None:
            self.worker.interupt()
        
        # if self.thread is not None:
        #     self.thread.terminate()
        
        return QtGui.QDialog.reject(self)

    def workerErorrProcess(self, msg):
        QtGui.QMessageBox.critical(self, "Export error", msg)

    def changeProgressStatus(self, status):
        # Plugin().plPrint(">>> " + status)
        # if showProgressAs == u"percent":
        #     self.__pbar.setFormat(status + " (%p%)")
        # elif showProgressAs == u"step":
        #     self.__pbar.setFormat(status + " (%v from %m)")
        # else:
        #     self.__pbar.setFormat(status)

        # self.__pbar.setRange(min, max)
        self.__statusLable.setText(status)

    def changeProgress(self, value):
        # self.__pbar.setValue(value)
        pass

    def addLayer(self):
        self.layerSrcCreated.emit(self.sqliteDB.getValue())
