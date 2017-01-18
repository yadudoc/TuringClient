import pickle
import serialize

class KottaFn(object):
    """
    This is modelled off of ipython parallel's RemoteFunction code
    view has to have some context of kotta
    """
    def __init__ (self, conn, f, block=True, **flags):
        self.__name__ = f.__name__
        self.__doc__  = f.__doc__
        self.__signature__ = getattr(f, '__signature__', None)  
        self.conn     = conn
        self.func     = f
        self.flags    = flags
      
    @staticmethod
    def dump_to_file(obj, filename):
        with open("{0}.pkl".format(filename), 'w') as sfile:
            #sfile.write(fn_buf)
            pickle.dump(obj, sfile)       

    def __call__ (self, *args, **kwargs) :
        """
        print("Submitting func: {0}".format(self.func))
        print("Args           : {0}".format(args))
        print("Kwargs         : {0}".format(kwargs))
        print("globals        : {0}".format(globals().keys()))
        """
        for item in globals():
            print(item, type(item))
        
        fn_buf = serialize.pack_apply_message(self.func, args, kwargs,
                                        buffer_threshold=1024*1024,
                                        item_threshold=1024)
        #print(fn_buf, len(fn_buf))
        
        with open("{0}.pkl".format(uuid.uuid4()), 'wb') as sfile:            
            pickle.dump(fn_buf, sfile)

        
        
        return fn_buf
    
def kottajob(conn, queue, walltime, block=True, **flags):
    """
    kottajob decorator
    
    """
    print (conn, queue, walltime)
    
    def kottajob_fn(f):
        print("In the inner function with arg f : {0}".format(f))
        print(dir(f))
        return KottaFn(conn, f, block, **flags)
    
    return kottajob_fn
