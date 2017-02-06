""" Class encapsulating a Kotta Job.

More documentation on this is here :
http://docs.cloudkotta.org/userguide/kottalibrary.html#kottajob-class

"""

import pickle
import copy
import time
import logging

from .kotta_outputs import KOut

logger  = logging.getLogger(__name__)

class KottaJob(object):
    """ KottaJob object represents a job on Kotta

    Provides methods to initialize, execute and track a job via Kotta

    """

    def __init__ (self, **kwargs) :
        """ Create the job
        """
        # Setting defaults
        self.__job_desc = {'inputs'   : '',
                           'outputs'  : '',
                           'walltime' : 300,
                           'queue'    : 'Test',
                           'output_file_stdout' : 'STDOUT.txt',
                           'output_file_stderr' : 'STDERR.txt',
                          }

        self.__job_id     = []
        self.__status     = 'unsubmitted'
        self.__valid_stati= ['unsubmitted', 'pending', 'staging_inputs', 'cancelled',
                             'completed', 'failed', 'processing', 'staging_outputs']
        self.__job_desc.update(kwargs)



    def submit(self, kconn):
        """ Submit job to Kotta.
        Returns True/False depending on whether the job submission succeeded.
        """
        response  = kconn.submit_task(self.__job_desc)
        if response['status'] == "Success":
            self.job_id = response['job_id']
            self.__status = 'pending'
            return True

        else:
            print("[ERROR] Job submission failed : {0}".format(response.get('reason', response)))
            return False

    def cancel(self, kconn):
        """ This method is not implemented
        """
        raise NotImplementedError

    def wait(self, kconn, maxwait=600, sleep=2, silent=True):
        """ Wait for job completion upto a maxtime duration.

        """
        for _ in range(int(maxwait/sleep)):
            if not silent:
                logger.debug("waiting on %s ", self.job_id)
            cur_status = self.status(kconn)
            if cur_status in [ 'completed', 'cancelled', 'failed' ]:
                return cur_status
            time.sleep(sleep)

        return False

    def status(self, kconn):
        """ Get status of a submitted job
        """
        if self.job_id :
            status = kconn.status_task(self.job_id)
            if self.__status == status.get('status'):
                return self.__status

            self.set_status(status.get('status'))

            if 'outputs' in status:
                logger.debug("Received new status")
                outputs = []
                for output in status['outputs']:
                    kout = KOut(output)

                    if kout.file.endswith('STDOUT.txt'):
                        status['STDOUT'] = kout
                        logger.debug("Set status for statas['STDOUT']: %s", kout)

                    elif kout.file.endswith('STDERR.txt'):
                        status['STDERR'] = kout
                        logger.debug("Set status for status['STDERR']: %s", kout)

                    else:
                        outputs.extend([kout])

                status['outputs'] = outputs

            self.__job_desc.update(status)
            logger.debug(self.__job_desc)
        return self.__status

    def set_status(self, status_string):
        """ Only to be used internally.
        Set's the status of the job *if* the status is a valid status string
        """
        if status_string in self.__valid_stati:
            self.__status = status_string
        else:
            print("[ERROR] Invalid Status : {0}".format(status_string))
            raise TypeError

    @property
    def jobname(self):
        """ Return the friendly name of the job
        """
        return self.desc.get('jobname', self.job_id)

    @property
    def outputs(self):
        """ Returns the outputs list of the job
        """
        return self.desc.get('outputs', [])

    #######################################################################################
    # Property desc managing __job_desc
    def set_desc(self, desc_dict):
        """ Update the description of the job from the desc_dict.
        """
        self.__job_desc.update(desc_dict)

    def get_desc(self):
        """ Return the job_desc dict
        """
        return self.__job_desc


    desc = property(get_desc, set_desc, None, "Description of the Kotta-Job")
    #######################################################################################
    #  Property job_id managing __job_id
    def get_job_id(self):
        """ Returns the last valid job_id
        """
        if self.__job_id :
            return self.__job_id[-1]
        else:
            return None

    def set_job_id(self, jobid):
        """ Set a job id. Only to be called internally.
        Note that __job_id is a list. This allows for tracking of past submissions
        of the same job object, including it's job_desc from kotta.
        """
        self.__job_id.extend([jobid])

    def del_job_id(self):
        """ Delete all job_id's
        """
        self.__job_id = []


    job_id = property(get_job_id, set_job_id, del_job_id, "Job identifier for the Kotta-Job")
    #######################################################################################

    def __deepcopy__(self, memo):
        """ Deepcopy method to ensure that deepcopy works correctly for job objects
        """
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for key, val in self.__dict__.items():
            setattr(result, key, copy.deepcopy(val, memo))
        return result


    def add_inputs(self, inputs):
        """ Always add inputs via this method
        """
        if not inputs:
            return

        if self.__job_desc['inputs']:
            self.__job_desc['inputs'] = self.__job_desc['inputs'] + ',' + ','.join(inputs)
        else:
            self.__job_desc['inputs'] = ','.join(inputs)

    def add_outputs(self, outputs):
        """ Always add outputs via this method
        """
        if not outputs:
            return

        if self.__job_desc['outputs']:
            self.__job_desc['outputs'] = self.__job_desc['outputs'] + ',' + ','.join(outputs)
        else:
            self.__job_desc['outputs'] = ','.join(outputs)

    @property
    def STDOUT(self):
        """ If STDOUT.txt is available, return the read string
        else: return a warn string
        """
        if self.__status == "completed":

            if 'STDOUT' in self.__job_desc:
                return self.__job_desc['STDOUT'].read()
            else:
                logger.warning(" STDOUT not found in self.desc")

        return None

    @property
    def STDERR(self):
        """ If STDERR is available, return as string
        else: Return None
        """
        if self.__status == "completed":
            if 'STDERR' in self.desc:
                return self.desc['STDERR'].read()
            else:
                logger.warning("STDERR not found in self.desc")

        return None

    def get_results(self, return_file='out.pkl'):
        """ Fetch results from S3.
        """
        if self.__status == "completed":
            #print(self.job.outputs)
            results  = [output for output in self.outputs if output.file == return_file ]
            if results:
                for result in results:
                    try:
                        result.fetch()
                    except Exception as e:
                        logger.error("Failed to download result %s", e)
                        print("Returning job object for inspection")
                        return None

                    return pickle.load(open(result.file, 'rb'))

            else:
                logger.error("Job had no results %s", self.__status)
                return None
        else:
            logger.warning("WARN: Job status != completed")
            return None
