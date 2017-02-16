#!/usr/bin/env python3
"""
Remote side executor code.

Executes a pickled package of a function and it's arguments and returns it's result objects
in a pickled file.
"""

import argparse
import pickle

from serialize import unpack_apply_message


def execute(inputfile, outputfile):
    """ Executor.
    Args: name of the inputfile, which is a pickled object that contains
    the function to be executed, it's args, kwargs etc.
    name of the outputfile, where the outputs from the computation are to
    be pickled are written

    """

    all_names = dir(__builtins__)
    user_ns   = locals()
    user_ns.update( {'__builtins__' : {k : getattr(__builtins__, k)  for k in all_names} } )

    bufs = None
    with open(inputfile, 'rb') as pickled_bufs:
        bufs = pickle.load(pickled_bufs)

    f, args, kwargs = unpack_apply_message(bufs, user_ns, copy=False)

    fname = getattr(f, '__name__', 'f')
    prefix     = "kotta_"
    fname      = prefix+"f"
    argname    = prefix+"args"
    kwargname  = prefix+"kwargs"
    resultname = prefix+"result"

    user_ns.update({ fname : f,
                     argname : args,
                     kwargname : kwargs,
                     resultname : resultname })

    code = "{0} = {1}(*{2}, **{3})".format(resultname, fname,
                                           argname, kwargname)

    try:

        print("[Kotta_DBG] Executing : {0}".format(code))
        print("[Kotta_DBG] Func      : {0}".format(f))
        exec(code, user_ns, user_ns)

    finally :
        #print("Done : {0}".format(locals()))
        print("[Kotta_DBG] Result    : {0}".format(user_ns.get(resultname)))
        with open(outputfile, 'wb') as outfile:
            pickle.dump(user_ns.get(resultname), outfile)

    return True



if __name__ == "__main__":

    parser   = argparse.ArgumentParser()
    parser.add_argument("-i", "--inputfile",  help="Serialized input", required=True)
    parser.add_argument("-o", "--outputfile", help="Serialized output file")
    parser.add_argument("-v", "--verbose",  dest='verbose',
                        action='store_true', help="Verbose output")
    args   = parser.parse_args()

    execute(args.inputfile, args.outputfile)
