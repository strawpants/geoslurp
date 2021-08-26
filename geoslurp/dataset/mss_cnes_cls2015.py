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


class mss_cls2015(MotuGridsBase):
    """Downloads the mean sea surface heigth data as netcdf"""
    scheme='altim'
    variables=["mss","mss_err"]
    preview={"bandnr":1,"bandname":variables[0]}
    # tiles=[1000,1000]
    # python motuclient.py -u roelof@geod.uni-bonn.de -p your_password(1) -m https://motu.aviso.altimetry.fr/motu-web/Motu -s AvisoMSS -d dataset-mss-cnes-cls15-global -x 20 -X 120 -y -75 -Y 30 -t "2015-01-01" -T "2015-01-01" --outputWritten netcdf4 -v sea_surface_height_above_reference_ellipsoid -v mss_err -o your_output_directory(1) -f your_output_file_name(1) --proxy-server=your_proxy_server_url:your_proxy_port_number(2) --proxy-user=your_proxy_user_login(3) --proxy-pwd=your_proxy_user_password(3)
    authalias="avisoftp"
    moturoot="https://motu.aviso.altimetry.fr/motu-web/Motu"
    motuservice="AvisoMSS"
    motuproduct="dataset-mss-cnes-cls15-global"

    # regularblocking = True
    # overviews=[8]
    #[ulx,xres,xskew,uly,yskew,yres]
    # geotransform=[0,0,1/60,84,-1/60,0]
    def __init__(self,dbconn):
        super().__init__(dbconn)

geoslurpCatalogue.addDataset(mss_cls2015)
