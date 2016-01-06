# -*- coding: utf-8 -*-
def classFactory(iface):
    from plugin import Lesis2sqlite
    return Lesis2sqlite(iface)
