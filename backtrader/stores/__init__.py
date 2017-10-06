#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015, 2016, 2017 Daniel Rodriguez
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

# The modules below should/must define __all__ with the objects wishes
# or prepend an "_" (underscore) to private classes/variables

try:
    from .ibstore import IBStore
except ImportError:
    pass  # The user may not have ibpy installed

try:
    from .vcstore import VCStore
except ImportError:
    pass  # The user may not have a module installed

try:
    from .oandastore import OandaStore
except ImportError:
    pass  # The user may not have a module installed

try:
    from .gdaxstore import GDAXStore
except ImportError:
    pass  # The user may not have a module installed

from .pnxstore import PNXStore
from .krakenstore import KrakenStore
from .ccstore import CCStore
from .vchartfile import VChartFile
