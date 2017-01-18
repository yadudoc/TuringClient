import os
import pickle
import serialize
import uuid
import json

import requests
import json
import pycurl
from urllib.parse import urlparse
#import urlparse # update to urllib.urlparse for python3

class bcolors:
    HEADER    = '\033[95m'
    OKBLUE    = '\033[94m'
    OKGREEN   = '\033[92m'
    WARNING   = '\033[93m'
    FAIL      = '\033[91m'
    ENDC      = '\033[0m'
    BOLD      = '\033[1m'
    UNDERLINE = '\033[4m'
    
GLOBAL_VERBOSE = True
def debug_print(string):
    if GLOBAL_VERBOSE :
        print(string)

class Kotta(object):
    """
    This class connects to the Kotta system. It keeps track of the credentials    
    and provides means to submit and track kotta jobs.   
    """
    __server_url = "http://ec2-52-2-217-165.compute-1.amazonaws.com:8888"
    __submit_url = __server_url + "/rest/v1/submit_task"
    __status_url = __server_url + "/rest/v1/status_task"
    __cancel_url = __server_url + "/rest/v1/cancel_task"
    __list_url   = __server_url + "/rest/v1/list_tasks"
    __upload_url = __server_url + "/rest/v1/upload_url"
    
    # __init__ expects a string for creds
    def __init__(self, creds):
        if isinstance(creds, str):
            self._creds = json.loads(creds)
        elif isinstance(creds, dict):
            self._creds = creds
        
    def status_task(self, jobid):
        debug_print("Status task : {0}".format(jobid))
        status = {}
        record = requests.get(self.__status_url + "/{0}".format(jobid))
        if record.status_code != 200:
            print("[ERROR] Failed to fetch job, please check jobid")
            return(status)

        results = record.json()
        status['status'] = results['status']
        
        #print(results['items'])
        for index in results['items']:
            k = list(results['items'][index].keys())[0]
            v = results['items'][index][k]
            if k in ['inputs', 'outputs']:
                if k not in status:
                    status[k] = []
                status[k].extend([v])
            
            else:
                status[k] = v
        
        return status
    
    def submit_task(self, task_desc):        
        task_desc["access_token"]  = self._creds.get("access_token")
        task_desc["refresh_token"] = self._creds.get("refresh_token")        
        r = requests.post(self.__submit_url, data=task_desc)        
        return r.json()
    
    @staticmethod
    def _upload (url, filepath):
        os.system("curl --request PUT --upload-file {0} '{1}' ".format(filepath, url))
        return 
        
    def upload_file(self, path):
        
        # Get a signed url
        c = { "access_token" : self._creds.get("access_token"),
              "refresh_token" : self._creds.get("refresh_token"),
              "filepath" : path
             }
        r = requests.post(self.__upload_url, data=c)        
        response = r.json()
        data_loc = None
        if r.status_code != 200:
            print ("ERROR: Failed to upload data :\n {0}".format(response.get('reason', 'Unknown')))            
            return -1
        else:
            self._upload(response.get('upload_url'), path)
            #r = requests.put(response.get('upload_url'), files=files )
            #if r.status_code != 200 :
            #    print("Upload failed due to : \n {0}".format(r.text))
            #    return None
            parsed =  urlparse(response.get('upload_url')) #.split('?')[0]
            s3_url =  "s3://{0}{1}".format(parsed.netloc.split('.')[0],
                                           parsed.path)
        return s3_url
            
