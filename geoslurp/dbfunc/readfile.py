from geoslurp.dbfunc.dbfunc import DBFunc
from geoslurp.config.catalogue import geoslurpCatalogue


class readfile(DBFunc):
    """Registers a plpython function which can read a file from the server and return a bytestring"""
    language="plpython3u"
    inargs="uri text"
    outargs="bytea"
    pgbody="import os\n" \
        "import re\n" \
        "pgroot=plpy.execute(\"select conf->>'pg_geoslurpmount' as pgroot from admin.settings_default\")[0][\"pgroot\"]\n" \
        "rlpath=os.path.realpath(uri.replace('${LOCALDATAROOT}/',pgroot))\n" \
        "allowedp=plpy.execute(\"select jsonb_array_elements_text(data->'allowedpaths') as apath from admin.inventory where pgfunc = 'readfile'\",1)\n" \
        "for pth in allowedp[0][\"apath\"]:\n" \
        "   plpy.info(pth+ \" \"+uri)\n" \
        "   if re.search(\"^\"+pth,rlpath):\n" \
        "       with open(rlpath,'rb') as fid:\n" \
        "           return fid.read()\n" \
        "raise plpy.Error(\"readfile() is not allowed to read files from this path\")"
    def __init__(self,dbcon):
        super().__init__(dbcon)
    def register(self,allowedpaths=None):
        """Register the readfile function together with allowed paths to read files from
        Parameters
        ----------
        allowedpaths: listlike
        listlike variable containing paths where this function is allowed to read files from
        """

        if allowedpaths:
            #update or create new entry
            allpaths=allowedpaths
            if "allowedpaths" in self._dbinvent.data:
                #append old paths and make unique
                allpaths.extend(self._dbinvent.data["allowedpaths"])
                allpaths=list(set(allpaths))

            self._dbinvent.data["allowedpaths"]=allpaths
        elif not "allowedpaths" in self._dbinvent.data:
            raise RuntimeError("Allowedpaths must be specified when first registering the readfile function")
       
        super().register()

geoslurpCatalogue.addDbFunc(readfile)
