"""Exceptions raised by Apps."""
from functools import wraps
from typing import List, Union, Callable, Any
from types import TracebackType

import dill
import logging
from tblib import Traceback
from typing import cast, List, TypeVar, Type

from six import reraise

from parsl.data_provider.files import File

# vs PR 1846: benc-mypy imports File from the data_provider module
# more directly, potentially to avoid import loops from trying to
# import the top level "parsl" here.
# from parsl import File

# vs PR 1846: see TypeVar playing in response to PR 1846 TODO

logger = logging.getLogger(__name__)


class ParslError(Exception):
    """Base class for all exceptions.

    Only to be invoked when a more specific error is not available.
    """


class AppException(ParslError):
    """An error raised during execution of an app.

    What this exception contains depends entirely on context
    """


class AppBadFormatting(ParslError):
    """An error raised during formatting of a bash function.
    """


class BashExitFailure(AppException):
    """A non-zero exit code returned from a @bash_app

    Contains:
    reason(str)
    exitcode(int)
    """

    def __init__(self, reason: str, exitcode: int) -> None:
        self.reason = reason
        self.exitcode = exitcode


class AppTimeout(AppException):
    """An error raised during execution of an app when it exceeds its allotted walltime.
    """


class BashAppNoReturn(AppException):
    """Bash app returned no string.

    Contains:
    reason(string)
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class MissingOutputs(ParslError):
    """Error raised at the end of app execution due to missing output files.

    Contains:
    reason(string)
    outputs(List of strings/files..)
    """

    # vs PR 1846:  I use List[File] for outputs; this PR uses a union of str or File
    # That might be because I've done other tidyup work regarding strings and files?

    # def __init__(self, reason: str, outputs: List[Union[str, File]]) -> None:
    def __init__(self, reason: str, outputs: List[File]) -> None:
        super().__init__(reason, outputs)
        self.reason = reason
        self.outputs = outputs

    def __repr__(self) -> str:
        return "Missing Outputs: {0}, Reason:{1}".format(self.outputs, self.reason)


class BadStdStreamFile(ParslError):
    """Error raised due to bad filepaths specified for STDOUT/ STDERR.

    Contains:
       reason(string)
       outputs(List of strings/files..)
       exception object
    """

    # TODO: [typing] This exception is never constructed with the first argument as a list. There
    # are two spots where this constructor is called from:
    #   - parsl.utils.get_std_fname_mode: invoked as __init__(message, exception)
    #   - parsl.app.bash.open_std_fd: invoked as __init__(filename, exception)

    # vs PR 1846  - seems like outputs is perhaps just a str and never the richer/conflicting
    # type discussed in PR 1846 TODO directly above?

    def __init__(self, outputs: str, exception: Exception) -> None:
        super().__init__(outputs, exception)
        self._outputs = outputs
        self._exception = exception

    def __repr__(self) -> str:
        return "FilePath: [{}] Exception: {}".format(self._outputs,
                                                     self._exception)

    def __str__(self) -> str:
        return self.__repr__()


class RemoteExceptionWrapper:
    def __init__(self, e_type: type, e_value: Exception, traceback: TracebackType) -> None:

        self.e_type = dill.dumps(e_type)
        self.e_value = dill.dumps(e_value)
        self.e_traceback = Traceback(traceback)

    def reraise(self) -> None:

        t = dill.loads(self.e_type)

        # the type is logged here before deserialising v and tb
        # because occasionally there are problems deserialising the
        # value (see #785, #548) and the fix is related to the
        # specific exception type.
        logger.debug("Reraising exception of type {}".format(t))

        v = dill.loads(self.e_value)
        tb = self.e_traceback.as_traceback()

        reraise(t, v, tb)


# vs PR 1846: PR 1846 makes wrap_error go from any callable to any callable
# and typechecks without casts.

# The benc-mypy version instead uses a type
# variable which should make the wrapped function have the same type as the
# supplied function - but the typechecker cannot see that from the implementation
# because I think @wraps doesn't pass the right types through (and maybe there's
# no reason it *should* believe that in general) so there is a cast at the
# end of the function now to make that type assertion.

# I am extremely suspicious of this TypeVar declaration - especially because
# I've put a cast in.

G = TypeVar("G")
F = TypeVar("F", bound=Callable[..., Union[G, RemoteExceptionWrapper]])

def wrap_error(func: F) -> F:
    @wraps(func)
    def wrapper(*args: object, **kwargs: object) -> Any:
        import sys
        from parsl.app.errors import RemoteExceptionWrapper
        try:
            return func(*args, **kwargs)
        except Exception:
            return RemoteExceptionWrapper(*sys.exc_info())

    return cast(F, wrapper)
