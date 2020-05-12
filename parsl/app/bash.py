from functools import update_wrapper
from inspect import signature, Parameter

from parsl.app.errors import wrap_error
from parsl.app.app import AppBase
from parsl.dataflow.dflow import DataFlowKernelLoader


def remote_side_bash_executor(func, *args, **kwargs):
    """Executes the supplied function with *args and **kwargs to get a
    command-line to run, and then run that command-line using bash.
    """
    import os
    import time
    import subprocess
    import logging
    import parsl.app.errors as pe
    from parsl import set_file_logger
    from parsl.utils import get_std_fname_mode

    logbase = "/tmp"
    format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    # make this name unique per invocation so that each invocation can
    # log to its own file. It would be better to include the task_id here
    # but that is awkward to wire through at the moment as apps do not
    # have access to that execution context.
    t = time.time()

    logname = __name__ + "." + str(t)
    logger = logging.getLogger(logname)
    set_file_logger(filename='{0}/bashexec.{1}.log'.format(logbase, t), name=logname, level=logging.DEBUG, format_string=format_string)
    logger.debug("In remote-side bash executor")
    func_name = func.__name__

    executable = None

    # Try to run the func to compose the commandline
    try:
        # Execute the func to get the commandline
        executable = func(*args, **kwargs)

    except AttributeError as e:
        if executable is not None:
            raise pe.AppBadFormatting("App formatting failed for app '{}' with AttributeError: {}".format(func_name, e))
        else:
            raise pe.BashAppNoReturn("Bash app '{}' did not return a value, or returned None - with this exception: {}".format(func_name, e), None)

    except IndexError as e:
        raise pe.AppBadFormatting("App formatting failed for app '{}' with IndexError: {}".format(func_name, e))
    except Exception as e:
        logger.error("Caught exception during formatting of app '{}': {}".format(func_name, e))
        raise e

    logger.debug("Executable: %s", executable)

    # Updating stdout, stderr if values passed at call time.

    def open_std_fd(fdname):
        # fdname is 'stdout' or 'stderr'
        stdfspec = kwargs.get(fdname)  # spec is str name or tuple (name, mode)
        if stdfspec is None:
            return None

        fname, mode = get_std_fname_mode(fdname, stdfspec)
        try:
            if os.path.dirname(fname):
                os.makedirs(os.path.dirname(fname), exist_ok=True)
            fd = open(fname, mode)
        except Exception as e:
            raise pe.BadStdStreamFile(fname, e)
        return fd

    logger.debug("In remote-side bash executor 2")
    std_out = open_std_fd('stdout')
    logger.debug("In remote-side bash executor 3")
    std_err = open_std_fd('stderr')
    logger.debug("In remote-side bash executor 4")
    timeout = kwargs.get('walltime')
    logger.debug("In remote-side bash executor 5")

    if std_err is not None:
        print('--> executable follows <--\n{}\n--> end executable <--'.format(executable), file=std_err, flush=True)

    returncode = None
    logger.debug("In remote-side bash executor 6 - about to run")
    try:
        proc = subprocess.Popen(executable, stdout=std_out, stderr=std_err, shell=True, executable='/bin/bash')
        logger.debug("In remote-side bash executor 7 process launched. now waiting. pid is {}".format(proc.pid))
        proc.wait(timeout=timeout)
        logger.debug("In remote-side bash executor 8 process launched. waiting over")
        returncode = proc.returncode
        logger.debug("In remote-side bash executor 9 process launched. getting return code")

    except subprocess.TimeoutExpired:
        logger.debug("In remote-side bash executor 10 timeoutexpired path")
        raise pe.AppTimeout("[{}] App exceeded walltime: {}".format(func_name, timeout))

    except Exception as e:
        logger.debug("In remote-side bash executor 11 exception path")
        raise pe.AppException("[{}] App caught exception with returncode: {}".format(func_name, returncode), e)

    logger.debug("In remote-side bash executor 12 no exception path")
    if returncode != 0:
        logger.debug("In remote-side bash executor 12 non-zero return code path")
        raise pe.AppFailure("[{}] App failed with exit code: {}".format(func_name, proc.returncode), proc.returncode)

    # TODO : Add support for globs here

    logger.debug("In remote-side bash executor 13 looking at outputs")
    missing = []
    for outputfile in kwargs.get('outputs', []):
        fpath = outputfile
        if type(outputfile) != str:
            fpath = outputfile.filepath

        if not os.path.exists(fpath):
            missing.extend([outputfile])

    logger.debug("In remote-side bash executor 14 done with outputs")
    if missing:
        logger.debug("In remote-side bash executor 15 missing outputs path")
        raise pe.MissingOutputs("[{}] Missing outputs".format(func_name), missing)

    logger.debug("In remote-side bash executor 16 normal return path")
    return returncode


class BashApp(AppBase):

    def __init__(self, func, data_flow_kernel=None, cache=False, executors='all', ignore_for_checkpointing=[]):
        super().__init__(func, data_flow_kernel=data_flow_kernel, executors=executors, cache=cache, ignore_for_checkpointing=ignore_for_checkpointing)
        self.kwargs = {}

        # We duplicate the extraction of parameter defaults
        # to self.kwargs to ensure availability at point of
        # command string format. Refer: #349
        sig = signature(func)

        for s in sig.parameters:
            if sig.parameters[s].default != Parameter.empty:
                self.kwargs[s] = sig.parameters[s].default

    def __call__(self, *args, **kwargs):
        """Handle the call to a Bash app.

        Args:
             - Arbitrary

        Kwargs:
             - Arbitrary

        Returns:
                   App_fut

        """
        invocation_kwargs = {}
        invocation_kwargs.update(self.kwargs)
        invocation_kwargs.update(kwargs)

        if self.data_flow_kernel is None:
            dfk = DataFlowKernelLoader.dfk()
        else:
            dfk = self.data_flow_kernel

        app_fut = dfk.submit(wrap_error(update_wrapper(remote_side_bash_executor, self.func)),
                             self.func, *args,
                             executors=self.executors,
                             fn_hash=self.func_hash,
                             cache=self.cache,
                             ignore_for_checkpointing=self.ignore_for_checkpointing,
                             **invocation_kwargs)

        return app_fut
