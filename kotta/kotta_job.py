import os
import pickle
import serialize
import uuid
import json

import requests
import json
import pycurl
import time
from urllib.parse import urlparse
#import urlparse # update to urllib.urlparse for python3
from .kotta_outputs import KOut
    
class KottaJob(object):
        
    def __init__ (self, **kwargs) :
        # Setting defaults
        self.__job_desc = {'inputs'   : [],
                           'outputs'  : [],
                           'walltime' : 300,
                           'queue'    : 'Test'
                     }
        self.__job_id     = []
        self.__status     = 'unsubmitted'
        self.__valid_stati= ['unsubmitted', 'pending', 'staging_inputs', 'cancelled',
                             'completed', 'failed', 'processing', 'staging_outputs']
        self.__job_desc.update(kwargs)
            
    def submit(self, Kconn):
        response  = Kconn.submit_task(self.__job_desc)        
        if response['status'] == "Success":
            self.job_id = response['job_id']
            self.__status = 'pending'
            return True
        else:
            print("[ERROR] Job submission failed : {0}".format(response.get('reason', response)))
            return False                
        
    def cancel(self, Kconn):
        raise NotImplementedError

    def wait(self, Kconn, maxwait=600, sleep=2 ):
        for i in range(int(maxwait/sleep)):
            cur_status = self.update_status(Kconn)
            if cur_status in [ 'completed', 'cancelled', 'failed' ]:
                return True
            time.sleep(sleep)
        
        return False

    def status(self, Kconn):
        print ("Status: Fn :")
        if self.job_id :
            st = Kconn.status_task(self.job_id)
            self.set_status(st.get('status'))
            if 'outputs' in st:
                
                #print ("Raw: ", st['outputs'])
                #print("*"*40)
                outputs = [KOut(o) for o in st['outputs']]
                st['outputs'] = outputs

            self.__job_desc.update(st)
        return self.__status

    def set_status(self, status_string):
        if status_string in self.__valid_stati:
            self.__status = status_string
        else:
            print("[ERROR] Invalid Status : {0}".format(status_string))
            raise TypeError

    @property
    def outputs(self):
        return self.desc.get('outputs', [])

    #######################################################################################
    """ Property desc managing __job_desc    
    """
    def set_desc(self, desc_dict):
        self.__job_desc.update(desc_dict)
        
    def get_desc(self):
        return self.__job_desc
    
    desc = property(get_desc, set_desc, None, "Description of the Kotta-Job")    
    #######################################################################################
    """ Property job_id managing __job_id
    """
    def get_job_id(self):
        if self.__job_id :
            return self.__job_id[-1]
        else:
            return None
        
    def set_job_id(self, jobid):        
        self.__job_id.extend([jobid])
        
    def del_job_id(self):
        self.__job_id = []
                
    job_id = property(get_job_id, set_job_id, del_job_id, "Job identifier for the Kotta-Job")        
    #######################################################################################

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            setattr(result, k, deepcopy(v, memo))
        return result
