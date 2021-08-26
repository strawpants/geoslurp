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
# License along with Frommle; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

# Author Roelof Rietbroek (roelof@geod.uni-bonn.de), 2018

from geoslurp.config.catalogue import geoslurpCatalogue
from geoslurp.dataset.motuGridsBase import MotuGridsBase

class Duacs(MotuGridsBase):
    """Downloads subsets of the ducacs gridded multimission altimeter datasets for given regions"""
    scheme='altim'
    variables=["sla","adt"]
    preview={"bandnr":1,"bandname":variables[0]}
    authalias="cmems"

    moturoot="http://my.cmems-du.eu/motu-web/Motu"
    # moturoot="http://my.cmems-du.eu/motu-web/Motu"
    motuservice="SEALEVEL_GLO_PHY_L4_REP_OBSERVATIONS_008_047-TDS"
    motuproduct="dataset-duacs-rep-global-merged-allsat-phy-l4"
    def __init__(self,dbconn):
        super().__init__(dbconn)


geoslurpCatalogue.addDataset(Duacs)
