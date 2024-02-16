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
from geoslurp.dataset.snrei import LLoveTable
from sqlalchemy import select

def setRefSystem(ds, refsys):
    # compute isomorphic frame parameter
    h1=ds.hn.loc[1].item()
    l1=ds.ln.loc[1].item()
    k1=ds.kn.loc[1].item()
    if refsys == 'CF':
        alpha = (h1 + 2 * l1) / 3
    elif refsys == 'CM':
        alpha = 1 + k1
    elif refsys == 'CE':
        alpha = k1
    elif refsys == 'CH':
        alpha = h1
    elif refsys == 'CL':
        alpha = l1
    
    # apply isomorphic frame parameter
    h1 -= alpha
    l1 -= alpha
    k1 -= alpha

    #plug the new values back in the dataset
    ds.hn.loc[1]=h1
    ds.ln.loc[1]=l1
    ds.kn.loc[1]=k1

    return ds


def getLoveNumbers(dbcon,name="PREM",ref=None):
    """Convenience function to retrieve a set of Love numbers from a geoslurp database"""

    ses=dbcon.Session()
    res=ses.query(LLoveTable).filter_by(name=name).first()
    
    #possibly change reference system
    if ref:
        if ref != res.ref:
            res.data=setRefSystem(res.data,ref)
            res.ref=ref
    return res
