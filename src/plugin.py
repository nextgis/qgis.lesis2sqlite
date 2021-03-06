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

from PyQt4 import QtCore, QtGui

from qgis.core import (
    QgsMapLayerRegistry,
    QgsApplication
)

from qgis_plugin_base import Plugin
from dialog import Lesis2SQLiteDialog

import resources_rc

class Lesis2sqlite(Plugin):
    def __init__(self, iface):
        Plugin.__init__(self, iface, "Lesis2sqlite")

        userPluginPath = QtCore.QFileInfo(QgsApplication.qgisUserDbFilePath()).path() + '/python/plugins/lesis2sqlite'
        systemPluginPath = QgsApplication.prefixPath() + '/python/plugins/lesis2sqlite'

        overrideLocale = QtCore.QSettings().value('locale/overrideFlag', False, type=bool)
        if not overrideLocale:
            localeFullName = QtCore.QLocale.system().name()[:2]
        else:
            localeFullName = QtCore.QSettings().value("locale/userLocale", "")

        if QtCore.QFileInfo(userPluginPath).exists():
            translationPath = userPluginPath + '/i18n/lesis2sqlite_' + localeFullName + '.qm'
        else:
            translationPath = systemPluginPath + '/i18n/lesis2sqlite_' + localeFullName + '.qm'

        self.localePath = translationPath
        if QtCore.QFileInfo(self.localePath).exists():
            self.translator = QtCore.QTranslator()
            self.translator.load(self.localePath)
            QgsApplication.installTranslator(self.translator)

    def initGui(self):
        self.plPrint("initGui")
        actionRun = self.addAction("Import", QtGui.QIcon(":/plugins/lesis2sqlite/icons/import.png"))
        actionRun.triggered.connect(self.run)

    def unload(self):
        self.delAllActions()

    def run(self):
        settings = QtCore.QSettings()

        dlg = Lesis2SQLiteDialog(self._iface.mainWindow())
        dlg.resize(settings.value("%s/dialog_size" % self._name, QtCore.QSize(200,100)))
        dlg.layerSrcCreated.connect(self.addLayer)
        dlg.exec_()

        settings.setValue("%s/dialog_size" % self._name, dlg.size())

    def addLayer(self, layerSrc):
        Plugin().plPrint("layerSrc: " + layerSrc)
        vlayer = self._iface.addVectorLayer(layerSrc, "lesis", "ogr")
        QgsMapLayerRegistry.instance().addMapLayer(vlayer)
