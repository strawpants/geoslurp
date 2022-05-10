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

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2022

import unittest
from geoslurp.config.slurplogger import slurplog, setDebugLevel
import numpy as np
import xarray as xr
import pandas as pd
from geoslurp.db import geoslurpConnect
import geoslurp.tools.xarray

def createToyDataset():
    """Takes the example dataset from https://docs.xarray.dev/en/stable/examples/weather-data.html"""

    np.random.seed(123)
    times = pd.date_range("2000-01-01", "2001-12-31", name="time")
    annual_cycle = np.sin(2 * np.pi * (times.dayofyear.values / 365.25 - 0.28))

    base = 10 + 15 * annual_cycle.reshape(-1, 1)
    tmin_values = base + 3 * np.random.randn(annual_cycle.size, 3)
    tmax_values = base + 10 + 3 * np.random.randn(annual_cycle.size, 3)

    ds = xr.Dataset(
        {
            "tmin": (("time", "location"), tmin_values),
            "tmax": (("time", "location"), tmax_values),
        },
        {"time": times, "location": ["IA", "IN", "IL"]},
    )

    return ds




class TestXarrayRW(unittest.TestCase):
    def test_save(self):
        gsconn=geoslurpConnect(readonly_user=False)
        #create a  simple xarray dataset to save
        dstest=createToyDataset()
        #where to save the data?
        tablename="testxrsave"
        dstest.gslrp.save(gsconn,tablename,"time", outofdb=False,overwrite=True)









if __name__ == '__main__':
    setDebugLevel()
    unittest.main()
