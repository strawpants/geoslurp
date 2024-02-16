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

# Author Roelof Rietbroek (r.rietbroek@utwente.nl), 2021
import cdsapi
from geoslurp.config.slurplogger import slurplogger
import time
from copy import copy
import numpy as np
import os
from urllib.error import HTTPError
class Cds:
    def __init__(self,resource,jobqueue={}):
        #start a client (which allows queing jobs in the bacjground)
        if not os.path.exists(os.path.join(os.path.expanduser("~"),".cdsapirc")):
            raise RuntimeError("Before using the cdsapi please visit https://cds.climate.copernicus.eu/api-how-to to obtain a token and setup your ~/.cdsapirc file")
        self.client = cdsapi.Client(wait_until_complete=False)
        self.resource=resource
        self.jobqueue=jobqueue
        self.requests=[] 

    def queueRequest(self,fout,requestDict):
        
        if os.path.exists(fout):
            slurplogger().info(f"Already downloaded file {fout}, skipping request")
            return
            
        req_id=None
        #possibly get the request id from a previously queued job
        if fout in self.jobqueue:
            req_id=self.jobqueue[fout]
            
            
        if req_id:
            #try to get an existing job
            slurplogger().info(f"Trying to retrieve previously queued job for {fout}")
            try:
                req=cdsapi.api.Result(self.client,dict(request_id=req_id))
                req.update()
            except: 
                #Job cannot be found anymore
                slurplogger().info(f"Job cannot be found anymore for {fout}, requeing")
                req_id=None
                del self.jobqueue[fout]

        if not req_id:
            #start a new request
            slurplogger().info(f"Queuing new CDS request for {fout}")
            req=self.client.retrieve(self.resource,requestDict)
            req.update()
            req_id=req.reply["request_id"]
            #add an entry to the inventory
            self.jobqueue[fout]=req_id
        
        self.requests.append((req,fout,req.reply["state"]))
    
    def clearRequests(self,removestates=['downloaded','unavailable','failed']):
        """clears certain requests and updates the jobqueue"""
        reqs=[]
        for req,fout,state in self.requests:
            if state in removestates:
                #clear from the jobqueue
                if fout in self.jobqueue:
                    del self.jobqueue[fout]
            else:
                reqs.append((req,fout,state))
        #update requests
        self.requests=reqs

    def downloadQueue(self,sleep=30):
        
        #wait for tasks to finish and download results to files
        nDownloaded=0
        nFailed=0
        while (nDownloaded+nFailed) < len(self.requests):
            # don't be too pushy and wait a while before checking
            time.sleep(sleep)
            for i,(req,fout,stateprev) in enumerate(self.requests):
                if stateprev == 'downloaded' or stateprev == 'unavailable':
                    #already downloaded the file in this queue or previous attempt failed
                    continue
                
                req.update()
                reply = req.reply
                req_id=reply["request_id"]
                state=reply['state']

                if state != stateprev:
                    slurplogger().info(f"Request ID: {req_id}, changed state from {stateprev} to:{state}")

                if state == "completed":
                    #download file
                    slurplogger().info(f"Downloading CDS request for {fout}")

                    try:
                        req.download(fout)
                        nDownloaded+=1
                        state="downloaded"
                    except HTTPError:
                        #resource may be gone in the meanwhile
                        nFailed+=1
                        slurplogger().error('Resource is not available anymore')
                        state="unavailable"

                elif state in ("failed",):
                    nFailed+=1
                    slurplogger().error(f'Message: {reply["error"].get("message")}')
                    slurplogger().error(f'Reason: {reply["error"].get("reason")}')
                    for n in (
                        reply.get("error", {}).get("context", {}).get("traceback", "").split("\n")
                    ):
                        if n.strip() == "":
                            break
                        slurplogger().error("  %s", n)
                    raise Exception(
                        f'reply["error"].get("message")  reply["error"].get("reason")'
                    )
                
                if state != stateprev:
                    self.requests[i]=(req,fout,state)

            slurplogger().info(f"Successful downloads: {nDownloaded}/{len(self.requests)}, failure: {nFailed}")
