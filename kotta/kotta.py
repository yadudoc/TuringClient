""" Kotta

Defines class Kotta that holds credentials to Kotta as well as the various accessor
functions such as submit, cancel, status.

"""
import os
import requests
import json
import logging
from urllib.parse import urlparse

logger  = logging.getLogger(__name__)

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
        """ Get the status of the task

        Args: jobid

        Returns: A Dict of results if job exists

        """
        logger.debug("Status task : %s", jobid)
        status = {}
        record = requests.get(self.__status_url + "/{0}".format(jobid))
        if record.status_code != 200:
            logging.error("Failed to fetch job, please check jobid")
            return status

        results = record.json()
        status['status'] = results['status']

        for index in results['items']:
            key_ = list(results['items'][index].keys())[0]
            val_ = results['items'][index][key_]
            if key_ in ['inputs', 'outputs']:
                if key_ not in status:
                    status[key_] = []
                status[key_].extend([val_])

            else:
                status[key_] = val_

        return status

    def submit_task(self, task_desc):
        """ Submit a task
        Args: task_desc which a dictionary
        Returns: The json object returned from Kotta

        """
        task_desc["access_token"]  = self._creds.get("access_token")
        task_desc["refresh_token"] = self._creds.get("refresh_token")
        res = requests.post(self.__submit_url, data=task_desc)
        return res.json()

    @staticmethod
    def _upload (url, filepath):
        os.system("curl --request PUT --upload-file {0} '{1}' ".format(filepath, url))
        return

    def upload_file(self, path):
        """ Upload a local file
        Uses temporary creds to request a signed url from Kotta.
        Uploads file to this location. This allows kotta to avoid having to route
        data traffic through our webserver.

        Args: Path to file

        """

        # Get a signed url
        creds = { "access_token" : self._creds.get("access_token"),
                  "refresh_token" : self._creds.get("refresh_token"),
                  "filepath" : path }

        r = requests.post(self.__upload_url, data=creds)
        response = r.json()
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
