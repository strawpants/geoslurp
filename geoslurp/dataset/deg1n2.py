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

from sqlalchemy import Column,Integer,String
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from geoslurp.dataset import DataSet
from geoslurp.datapull.ftp import Uri as ftp
from geoslurp.datapull.http import Uri as http
from datetime import datetime
from geoslurp.tools.time import decyear2dt
from geoslurp.config.slurplogger import slurplogger
import os
import tarfile
import re
from copy import deepcopy
from geoslurp.config.catalogue import geoslurpCatalogue
scheme="gravity"
GeocTBase=declarative_base(metadata=MetaData(schema=scheme))

class LowdegreeSource():
    meta={"data":{}}
    uri=''
    fout=''
    direc=''
    sqrt3timesRE=11047256.4063275
    order=['c10','c11','s11','c20']
    def __init__(self,direc,uri,lastupdate=None,order=None):
        self.meta['name']=self.__class__.__name__
        self.uri=uri
        self.direc=direc
        self.fout=self.meta['name']+"."+uri.split('.')[-1]
        if lastupdate:
            self.meta['lastupdate']=lastupdate
        if order:
            self.order=order

        self.meta['origin']='CF'

        self.meta['data']={"vars":self.order,"covstore":[],"time":[],"d12":[],"covUpper":[],"pot2geo":self.sqrt3timesRE}
        for i,el in enumerate(self.order):
            for j in range(i+1):
                self.meta['data']['covstore'].append((el,self.order[j]))

    def dindex(self,el):
        return self.order.index(el)

    def covindex(self,el1,el2):
        """Index function of the upper covariance matrix"""
        i=self.dindex(el1)
        j=self.dindex(el2)
        if i >=j:
            return int(((i+1)*i)/2+j)
        else:
            return int(((j+i)*j)/2+i)

    def download(self):
        if self.uri.startswith('http'):
            uri=http(self.uri,lastmod=self.meta['lastupdate'])
        elif self.uri.startswith('ftp'):
            uri=ftp(self.uri)
            uri.updateModTime()
        uri.download(self.direc,check=True,outfile=self.fout)
    def extract(self):
        #to be implmented in derived classes
        pass


class Rietbroeketal2016updated(LowdegreeSource):
    def __init__(self,direc):
        super().__init__(direc=direc,uri='https://wobbly.earth/data/Geocenter_dec2017.tgz',order=['c10','c11','s11'],lastupdate=datetime(2018,10,16))
    def extract(self):

        #set general settings
        citation="Rietbroek, R., Brunnabend, S.-E., Kusche, J., Schröter, J., Dahle, C., 2016. " \
                                      "Revisiting the Contemporary Sea Level Budget on Global and Regional Scales. " \
                                      "Proceedings of the National Academy of Sciences 201519132. " \
                                      "https://doi.org/10.1073/pnas.1519132113"

        tf=tarfile.open(os.path.join(self.direc,self.fout),'r:gz')

        metacomb=[]

        files=['Geocenter/GeocentCM-CF_Antarctica.txt',
               'Geocenter/GeocentCM-CF_Hydrology.txt',
               'Geocenter/GeocentCM-CF_LandGlaciers.txt',
               'Geocenter/GeocentCM-CF_GIA.txt',
               'Geocenter/GeocentCM-CF_TotSurfload.txt']

        valorder=['c11','s11','c10']
        covorder=[('c11','c11'),('s11','s11'),('c10','c10')]
        for file in files:
            #get files
            with tf.extractfile(file) as fid:
                singlemeta=deepcopy(self.meta)
                singlemeta['name']=self.meta['name']+'_'+file.split('_')[-1][:-4]
                singlemeta['data']["citation"]=citation
                time=[]
                for ln in fid:
                    lnspl=ln.decode('utf-8').split()
                    time.append(decyear2dt(float(lnspl[0])))
                    d12=[0]*3
                    for el,val in zip(valorder,lnspl[1:4]):
                        d12[self.dindex(el)]=float(val)/self.sqrt3timesRE

                    singlemeta["data"]['d12'].append(d12)
                    covUpper=[0]*6
                    for (el1,el2),val in zip(covorder,lnspl[4:7]):
                        covUpper[self.covindex(el1,el2)]=val
                    singlemeta["data"]['covUpper'].append(covUpper)

                singlemeta["data"]["time"]=[dt.isoformat() for dt in time]
                singlemeta['tstart']=min(time)
                singlemeta['tend']=max(time)
                singlemeta['lastupdate']=datetime.now()
                metacomb.append(singlemeta)

        return metacomb

class Sun2017Comb(LowdegreeSource):
    def __init__(self,direc,uri=None):
        if not uri:
            uri='https://d1rkab7tlqy5f1.cloudfront.net/CiTG/Over%20faculteit/Afdelingen/' \
                'Geoscience%20%26%20Remote%20sensing/Research/Gravity/models%20and%20data/3_Deg1_C20_CMB.txt'
        lastupdate=datetime(2018,1,1)
        super().__init__(direc=direc,uri=uri,lastupdate=lastupdate)
    def extract(self):
        singlemeta=self.meta

        citation="Reference: Yu Sun, Pavel Ditmar, Riccardo Riva (2017), Statistically optimal" \
                                       " estimation of degree-1 and C20 coefficients based on GRACE data and an " \
                                       "ocean bottom pressure model Geophysical Journal International, 210(3), " \
                                       "1305-1322, 2017. doi:10.1093/gji/ggx241."


        valorder=['c10','c11','s11']
        covorder=[('c10','c10'),('c10','c11'),('c10','s11'),('c10','c20'),('c11','c11'),('c11','s11'),('c11','c20'),('s11','s11'),('s11','c20'),('c20','c20')]

        singlemeta['data']["citation"]=citation
        with open(os.path.join(self.direc,self.fout),'rt') as fid:
            dataregex=re.compile('^ +[0-9]')
            time=[]
            for ln in fid:
                if dataregex.match(ln):
                    lnspl=ln.split()
                    time.append(decyear2dt(float(lnspl[0])))
                    d12=[0]*4
                    for el,val in zip(valorder,lnspl[1:4]):
                        d12[self.dindex(el)]=val
                    singlemeta['data']['d12'].append(d12)


                    covUpper=[0]*10
                    for (el1,el2),val in zip(covorder,lnspl[5:]):
                        covUpper[self.covindex(el1,el2)]=val
                    singlemeta['data']['covUpper'].append(covUpper)

        singlemeta["data"]["time"]=[dt.isoformat() for dt in time]
        singlemeta['tstart']=min(time)
        singlemeta['tend']=max(time)
        singlemeta['lastupdate']=datetime.now()

        return [singlemeta]

class Sun2017Comb_GIArestored(Sun2017Comb):
        def __init__(self,direc):
            uri="https://d1rkab7tlqy5f1.cloudfront.net/CiTG/Over%20faculteit/Afdelingen/" \
                     "Geoscience%20%26%20Remote%20sensing/Research/Gravity/models%20and%20data/" \
                     "4_Deg1_C20_CMB_GIA_restored.txt"
            super().__init__(direc,uri)

class GeocTable(GeocTBase):
    """Defines the Geocenter motion table"""
    __tablename__='deg1n2'
    id=Column(Integer,primary_key=True)
    name=Column(String,unique=True)
    lastupdate=Column(TIMESTAMP)
    tstart=Column(TIMESTAMP)
    tend=Column(TIMESTAMP)
    origin=Column(String)
    data=Column(JSONB)


class Deg1n2(DataSet):
    """Dataset registering several low degree (up to degree x) estimates including their definitions"""
    table=GeocTable
    dsources=[Rietbroeketal2016updated, Sun2017Comb, Sun2017Comb_GIArestored]
    scheme=scheme
    # "ftp://ftp.csr.utexas.edu/pub/slr/geocenter/"
    #     # {'name':'Rietbroeketal2016updated','uri':'https://wobbly.earth/data/Geocenter_dec2017.tgz','lastupdate':datetime(2018,10,16)},
    #     # {'name':'SwensonWahr2008','uri':'ftp://podaac.jpl.nasa.gov/allData/tellus/L2/degree_1/deg1_coef.txt','lastupdate':datetime(2018,10,16)},
    #     # {'name':'Sun2017Comb','uri':'https://d1rkab7tlqy5f1.cloudfront.net/CiTG/Over%20faculteit/Afdelingen/Geoscience%20%26%20Remote%20sensing/Research/Gravity/models%20and%20data/3_Deg1_C20_CMB.txt'
    #     #                             ,'lastupdate':datetime(2018,10,16)},
    # ]
    def __init__(self,dbconn):
        super().__init__(dbconn)
        # Create table if it doesn't exist
        self.table.metadata.create_all(self.db.dbeng, checkfirst=True)

    def pull(self):
        """Pulls known geocenter motion estimates from the internet and stores them in the cache"""
        for gsource in self.dsources:
            gsource(self.cacheDir()).download()



    def register(self):
        """"""
        for gsource in self.dsources:
            try:
               src=gsource(self.cacheDir())
               metadicts=src.extract()
               slurplogger().info("registering %s"%(src.meta["name"]))
               for meta in metadicts:
                   if self.entryNeedsUpdate(meta['name'],lastmod=src.meta['lastupdate'],col=self.table.name):
                        self.addEntry(meta)
            except Exception as e:
               #possibly not downloaded but that is ok
               continue

        self.updateInvent()


geoslurpCatalogue.addDataset(Deg1n2)

