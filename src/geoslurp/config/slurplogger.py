# This file is part of geoslurp.
# geoslurp is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.

# geoslurp is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public
# License along with geoslurp; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2018


import logging

from tqdm import tqdm


slurplog=logging.getLogger("Geoslurp")
# default is info level
slurplog.setLevel(logging.INFO)

ch = logging.StreamHandler()
# create formatter
formatter = logging.Formatter('%(name)s-%(levelname)s: %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
slurplog.addHandler(ch)
#this is needed to avoid double messages
slurplog.propagate=False

# geoslurp wide logger
def slurplogger():
    """Retrieve the geoslurp specific logger (deprecated, only for compatibility)"""
    return slurplog

def debugging():
    """Test if the logging level is set to DEBUG"""
    return slurplog.getEffectiveLevel() == logging.DEBUG

def setInfoLevel():
    """Set logging level to INFO severity"""
    slurplog.setLevel(logging.INFO)

def setDebugLevel():
    """Set logging level to DEBUG severity"""
    slurplog.setLevel(logging.DEBUG)


def setWarningLevel():
    """Set logging level to WARNING severity"""
    slurplog.setLevel(logging.WARNING)

def setErrorLevel():
    """Set logging level to ERROR severity"""
    slurplog.setLevel(logging.ERROR)



class TqdmLoggingHandler(logging.StreamHandler):
    """Avoid tqdm progress bar interruption by logger's output to console"""
    # see logging.StreamHandler.eval method:
    # https://github.com/python/cpython/blob/d2e2534751fd675c4d5d3adc208bf4fc984da7bf/Lib/logging/__init__.py#L1082-L1091
    # and tqdm.write method:
    # https://github.com/tqdm/tqdm/blob/f86104a1f30c38e6f80bfd8fb16d5fcde1e7749f/tqdm/std.py#L614-L620

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg, end=self.terminator)
        except RecursionError:
            raise
        except Exception:
            self.handleError(record)

slurplog.addHandler(TqdmLoggingHandler())
