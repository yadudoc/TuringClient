""" Wrapping python functions to conform to a KottaJob object.
"""

import pickle
import uuid
import os
import logging

import serialize
from .kotta_job import KottaJob

logger  = logging.getLogger(__name__)

class KottaFn(object):
    """
    This is modelled off of ipython parallel's RemoteFunction code
    view has to have some context of kotta
    """
    def __init__ (self, conn, job_desc, f, block=True, **flags):
        """ Construct a KottaFn object
        """
        self.__name__ = f.__name__
        self.__doc__  = f.__doc__
        self.__signature__ = getattr(f, '__signature__', None)
        self.conn     = conn
        self.func     = f
        self.flags    = flags
        self.job_desc = job_desc
        self.block    = block
        self.job      = None

    @staticmethod
    def dump_to_file(obj, filename):
        """ Pickle obj -> filename
        Perhaps these methods should move the KottaJob.
        I don't think this fn is going to be used.
        """
        with open("{0}.pkl".format(filename), 'w') as sfile:
            #sfile.write(fn_buf)
            pickle.dump(obj, sfile)

    def __call__ (self, *args, **kwargs) :
        """ Override the __call__ behavior to trap the args to the function,
        serialize and ship to a remote node via Kotta. Pickled results sent
        back are presented to the user.
        """

        self.job = KottaJob(**self.job_desc)

        if 'inputs' in kwargs:
            self.job.add_inputs(kwargs['inputs'])

        if 'outputs' in kwargs:
            self.job.add_outputs(kwargs['outputs'])

        fn_buf  = serialize.pack_apply_message(self.func, args, kwargs,
                                               buffer_threshold=1024*1024,
                                               item_threshold=1024)

        fn_id   = uuid.uuid4()
        pkl_dir = "pkl"
        if not os.path.exists(pkl_dir):
            os.makedirs(pkl_dir)
        fn_pkl  = "{0}/{1}.in.pkl".format(pkl_dir, fn_id)
        out_pkl = "{0}/out.pkl".format(pkl_dir)

        with open(fn_pkl, 'wb') as sfile:
            pickle.dump(fn_buf, sfile)

        s3_url  = self.conn.upload_file(fn_pkl)


        self.job.add_inputs([s3_url])
        self.job.add_outputs([os.path.basename(out_pkl)])
        self.job.desc = { 'jobname' : 'Kotta Ipython {0}'.format(self.func.__name__),
                          'executable' : '/bin/bash exec.sh {0} {1}'.format(os.path.basename(fn_pkl),
                                                                            os.path.basename(out_pkl))
                        }

        submit_st = self.job.submit(self.conn)
        if not submit_st:
            # We should ideally raise an exception here with the exception object
            # containing the job object.
            logging.error("Submit failed, returning job object")
            return self.job

        if self.block :
            # Blocking behavior. Wait for completion
            status = self.job.wait(self.conn)

            if status == "completed":
                #print(self.job.outputs)
                results  = [output for output in self.job.outputs if output.file == 'out.pkl' ]
                if results:
                    for result in results:

                        try:
                            result.fetch()
                        except Exception as e:
                            logging.error("ERROR: Failed to download result %s", e)
                            print("ERROR: Failed to download result")
                            print("Returning job object for inspection")
                            return (None, self.job)

                        return pickle.load(open(result.file, 'rb'))
                else:
                    print("ERROR: No result was captured")
                    logging.error("ERROR: No result was captured")

            else:
                logging.debug("Job did not complete successfully")

        else:
            # Non blocking. Return job object immediately
            logging.debug("Returning job object for %s:%s", self.job.job_id, self.job.status)

        return self.job


def kottajob(conn, queue, walltime, block=True, requirements='', inputs=[], **flags):
    '''     kottajob decorator

    While we still have inputs=[] as a kwargs in kottajob, it is bad form to use it
    since inputs should be defined at function call time. Similarly outputs=[] are no
    longer defined here for that reason.

    The major reason for removing outputs=[] is to avoid name conflicts in aggregation
    functions. Say, a fn() is called N times, and each fn() has the same outputs list,
    further aggregation is not possible due to the fact that feeding the similarly named 
    files to another fn() results in overwrites on the remote node.

    '''

    # Setup the requirements on the remote side
    req_string = '''cat <<EOF > requirements.txt
PyMySQL
jupyter
{0}
EOF
pip3 install -r requirements.txt
'''.format(requirements)

    exec_sh = '''#!/bin/bash
apt-get -y install python3 python3-pip
{0}
tar -xzf serialize.tar.gz
python3 runner.py -i $1 -o $2
'''.format(req_string)

    input_csl = 's3://klab-jobs/inputs/yadu/runner.py, s3://klab-jobs/inputs/yadu/serialize.tar.gz'
    if inputs:
        input_csl = input_csl + ',' + ','.join(inputs)

    job_desc  =  {'jobtype'            : 'script',
                  'inputs'             : input_csl,
                  'output_file_stdout' : 'STDOUT.txt',
                  'output_file_stderr' : 'STDERR.txt',
                  'script_name'        : 'exec.sh',
                  'script'             : exec_sh,
                  'walltime'           : walltime,
                  'queue'              : queue }

    logging.debug('Job desc : \n %s ', job_desc)

    def kottajob_fn(func):
        """ The func definition is used to construct a KottaFn object
        that has the func code. On __call__ the args to func are received,
        at which point we serialize the function and it's args for shipping.
        """
        return KottaFn(conn, job_desc, func, block, **flags)

    return kottajob_fn
