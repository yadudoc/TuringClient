import pickle
import serialize
import uuid
import os
from .kotta_job import KottaJob

class KottaFn(object):
    """
    This is modelled off of ipython parallel's RemoteFunction code
    view has to have some context of kotta
    """
    def __init__ (self, conn, job, f, block=True, **flags):
        self.__name__ = f.__name__
        self.__doc__  = f.__doc__
        self.__signature__ = getattr(f, '__signature__', None)  
        self.conn     = conn
        self.func     = f
        self.flags    = flags
        self.job      = job
        self.block    = block
      
    @staticmethod
    def dump_to_file(obj, filename):
        with open("{0}.pkl".format(filename), 'w') as sfile:
            #sfile.write(fn_buf)
            pickle.dump(obj, sfile)       

    def __call__ (self, *args, **kwargs) :
                
        fn_buf  = serialize.pack_apply_message(self.func, args, kwargs,
                                        buffer_threshold=1024*1024,
                                        item_threshold=1024)
        #print(fn_buf, len(fn_buf))
        fn_id   = uuid.uuid4()
        user    = os.environ['USER']
        pkl_dir = "pkl".format(user)
        if not os.path.exists(pkl_dir):
            os.makedirs(pkl_dir)
        fn_pkl  = "{0}/{1}.in.pkl".format(pkl_dir, fn_id)
        out_pkl = "{0}/out.pkl".format(pkl_dir, fn_id)

        #print("Debug pkl_file : {0}".format(fn_pkl))

        with open(fn_pkl, 'wb') as sfile:            
            pickle.dump(fn_buf, sfile)

        s3_url  = self.conn.upload_file(fn_pkl)

        #print("Uploaded url : ", s3_url)

        self.job.add_inputs([s3_url])
        self.job.add_outputs([os.path.basename(out_pkl)])
        self.job.desc = { 'jobname' : 'Kotta Ipython {0}'.format(self.func.__name__),
                          'executable' : '/bin/bash exec.sh {0} {1}'.format(os.path.basename(fn_pkl),
                                                                           os.path.basename(out_pkl))
                         }
        
        submit_st = self.job.submit(self.conn)
        if not submit_st:
            print ("ERROR: Submit failed, return job object")
            return self.job
        
        if self.block :
            status = self.job.wait(self.conn)

            if status == "completed": 
                #print(self.job.outputs)
                results  = [output for output in self.job.outputs if output.file == 'out.pkl' ]
                if results:
                    for result in results:

                        try:
                            result.fetch()
                        except Exception as e:
                            print("ERROR: Failed to download result")
                            print("Returning job object for inspection")
                            return (None, self.job)

                        return pickle.load(open(result.file, 'rb'))
                else:
                    print("ERROR: No result was captured")
                    return self.job
            else:
                print("Job {0}".format(status))
                print("Returning job object")
                return self.job

        else:
            return self.job

        return self.job
    
def kottajob(conn, queue, walltime, block=True, inputs=[], outputs=[], **flags):
    '''
    kottajob decorator    
    '''

    exec_sh = '''#!/bin/bash
    apt-get -y install python3 python3-pip
    pip3 install PyMySQL jupyter
    tar -xzf serialize.tar.gz
    python3 runner.py -i $1 -o $2
    '''
    job = KottaJob(jobtype     = 'script',
                   inputs      = 's3://klab-jobs/inputs/yadu/runner.py, s3://klab-jobs/inputs/yadu/serialize.tar.gz' ,
                   output_file_stdout = "STDOUT.txt",
                   output_file_stderr = "STDERR.txt",
                   script_name = 'exec.sh',
                   script      = exec_sh,
                   walltime    = walltime,
                   queue       = queue)
    job.add_inputs(inputs)
    #job.add_outputs(outputs)

    #print(job.desc)

    def kottajob_fn(f):
        #print("In the inner function with arg f : {0}".format(f))        
        return KottaFn(conn, job, f, block, **flags)
    
    return kottajob_fn
