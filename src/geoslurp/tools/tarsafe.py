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
# safer way to extract a tarball based on https://github.com/strawpants/geoslurp/pull/6


import os

def tar_safe_extractall(tarfid, outdir):
    abs_outdir=os.path.abspath(outdir)
    #check all members for tarbomb behavior
    for member in tarfid.getmembers():
        member_path = os.path.abspath(os.path.join(outdir, member.name))
        prefix = os.path.commonprefix([abs_outdir, member_path])
        if prefix != abs_outdir:
            raise Exception("Attempted Path Traversal in Tar File")

    tarfid.extractall(outdir) 
