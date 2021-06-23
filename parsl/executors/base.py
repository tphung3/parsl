from abc import ABCMeta, abstractmethod, abstractproperty
from concurrent.futures import Future
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence

from parsl.data_provider.staging import Staging

# for type checking:
from parsl.providers.provider_base import ExecutionProvider
from parsl.providers.provider_base import JobStatus

import parsl  # noqa F401


class ParslExecutor(metaclass=ABCMeta):
    """Executors are abstractions that represent available compute resources
    to which you could submit arbitrary App tasks.

    This is a metaclass that only enforces concrete implementations of
    functionality by the child classes.

    Can be used as a context manager. On exit, calls ``self.shutdown()`` with
    no arguments and re-raises any thrown exception.

    In addition to the listed methods, a ParslExecutor instance must always
    have a member field:

       label: str - a human readable label for the executor, unique
              with respect to other executors.

    An executor may optionally expose:

       storage_access: Sequence[parsl.data_provider.staging.Staging] - a sequence of staging
              providers that will be used for file staging. In the absence of this
              attribute, or if this attribute is `None`, then a default value of
              ``parsl.data_provider.staging.default_staging`` will be used by the
              staging code.
    """

    # mypy doesn't actually check that the below are defined by
    # concrete subclasses - see  github.com/python/mypy/issues/4426
    # and maybe PEP-544 Protocols

    label: str
    provider: ExecutionProvider
    managed: bool
    outstanding: Any  # what is this? used by strategy
    working_dir: Optional[str]
    storage_access: Optional[Sequence[Staging]]
    run_id: Optional[str]

    def __enter__(self) -> "ParslExecutor":
        return self

    # too lazy to figure out what the three Anys here should be
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        self.shutdown()
        return False

    @abstractmethod
    def start(self) -> Optional[List[str]]:
        """Start the executor.

        Any spin-up operations (for example: starting thread pools) should be performed here.
        """
        pass

    @abstractmethod
    def submit(self, func: Callable, resource_specification: Dict[str, Any], *args: Any, **kwargs: Dict[str, Any]) -> Future:
        """Submit.

        We haven't yet decided on what the args to this can be,
        whether it should just be func, args, kwargs or be the partially evaluated
        fn

        BENC: based on how ipp uses this, this follows the semantics of async_apply from ipyparallel.
        Based on how the thread executor works, its:

            https://docs.python.org/3/library/concurrent.futures.html
            Schedules the callable, fn, to be executed as fn(*args **kwargs) and returns a Future object representing the execution of the callable.

        These are consistent

        """
        pass

    @abstractmethod
    def scale_out(self, blocks: int) -> List[str]:
        """Scale out method.

        We should have the scale out method simply take resource object
        which will have the scaling methods, scale_out itself should be a coroutine, since
        scaling tasks can be slow.

        :return: A list of block ids corresponding to the blocks that were added.
        """
        pass

    @abstractmethod
    def scale_in(self, blocks: int) -> List[str]:
        """Scale in method.

        Cause the executor to reduce the number of blocks by count.

        We should have the scale in method simply take resource object
        which will have the scaling methods, scale_in itself should be a coroutine, since
        scaling tasks can be slow.

        MYPY branch notes: the scale in calls in strategy expect there to be many
        more parameters to this. This maybe could be resolved by treating a
        status providing executor as more generally a strategy-scalable
        executor, and having strategies statically typed to work on those.

        :return: A list of block ids corresponding to the blocks that were removed.
        """
        pass

    @abstractmethod
    def shutdown(self) -> bool:
        """Shutdown the executor.

        This includes all attached resources such as workers and controllers.
        """
        pass

    @abstractproperty
    def scaling_enabled(self) -> bool:
        """Specify if scaling is enabled.

        The callers of ParslExecutors need to differentiate between Executors
        and Executors wrapped in a resource provider
        """
        pass

    def create_monitoring_info(self, status: Dict[str, JobStatus]) -> List[object]:
        """Create a monitoring message for each block based on the poll status.

        TODO: block_id_type should be an enumerated list of valid strings, rather than all strings

        TODO: there shouldn't be any default values for this - when it is invoked, it should be explicit which is needed?
        Neither seems more natural to me than the other.

        TODO: internal vs external should be more clearly documented here

        :return: a list of dictionaries mapping to the info of each block
        """
        return []

    def monitor_resources(self) -> bool:
        """Should resource monitoring happen for tasks on running on this executor?

        Parsl resource monitoring conflicts with execution styles which use threads, and
        can deadlock while running.

        This function allows resource monitoring to be disabled per executor implementation.
        """
        return True

    @abstractmethod
    def status(self) -> Dict[str, JobStatus]:
        """Return the status of all jobs/blocks currently known to this executor.

        :return: a dictionary mapping block ids (in string) to job status
        """
        pass

    @property
    @abstractmethod
    def status_polling_interval(self) -> int:
        """Returns the interval, in seconds, at which the status method should be called. The
        assumption here is that, once initialized, an executor's polling interval is fixed.
        In practice, at least given the current situation, the executor uses a single task provider
        and this method is a delegate to the corresponding method in the provider.

        :return: the number of seconds to wait between calls to status() or zero if no polling
        should be done
        """
        pass

    @property
    @abstractmethod
    def error_management_enabled(self) -> bool:
        """Indicates whether worker error management is supported by this executor. Worker error
        management is done externally to the executor. However, the executor must implement
        certain methods that allow this to function. These methods are:

        Status Handling Methods
        -----------------------
        :method:handle_errors
        :method:set_bad_state_and_fail_all

        The basic idea of worker error management is that an external entity maintains a view of
        the state of the workers by calling :method:status() which is then processed to detect
        abnormal conditions. This can be done externally, as well as internally, through
        :method:handle_errors. If an entity external to the executor detects an abnormal condition,
        it can notify the executor using :method:set_bad_state_and_fail_all(exception).

        Some of the scaffolding needed for implementing error management inside executors,
        including implementations for the status handling methods above, is available in
        :class:parsl.executors.status_handling.StatusHandlingExecutor, which, interested executors,
        should inherit from. Noop versions of methods that are related to status handling and
        running parsl tasks through workers are implemented by
        :class:parsl.executors.status_handling.NoStatusHandlingExecutor.
        """
        pass

    @abstractmethod
    def handle_errors(self, error_handler: "parsl.dataflow.job_error_handler.JobErrorHandler",
                      status: Dict[str, JobStatus]) -> bool:
        """This method is called by the error management infrastructure after a status poll. The
        executor implementing this method is then responsible for detecting abnormal conditions
        based on the status of submitted jobs. If the executor does not implement any special
        error handling, this method should return False, in which case a generic error handling
        scheme will be used.
        :param error_handler: a reference to the generic error handler calling this method
        :param status: status of all jobs launched by this executor
        :return: True if this executor implements custom error handling, or False otherwise
        """
        pass

    @abstractmethod
    def set_bad_state_and_fail_all(self, exception: Exception) -> None:
        """Allows external error handlers to mark this executor as irrecoverably bad and cause
        all tasks submitted to it now and in the future to fail. The executor is responsible
        for checking  :method:bad_state_is_set() in the :method:submit() method and raising the
        appropriate exception, which is available through :method:executor_exception().
        """
        pass

    @property
    @abstractmethod
    def bad_state_is_set(self) -> bool:
        """Returns true if this executor is in an irrecoverable error state. If this method
        returns true, :property:executor_exception should contain an exception indicating the
        cause.
        """
        pass

    @property
    @abstractmethod
    def executor_exception(self) -> Exception:
        """Returns an exception that indicates why this executor is in an irrecoverable state."""
        pass

    @property
    @abstractmethod
    def tasks(self) -> Dict[object, Future]:
        """Contains a dictionary mapping task IDs to the corresponding Future objects for all
        tasks that have been submitted to this executor."""
        pass

    @property
    def run_dir(self) -> str:
        """Path to the run directory.
        """
        return self._run_dir

    @run_dir.setter
    def run_dir(self, value: str) -> None:
        self._run_dir = value

    @property
    def hub_address(self) -> Optional[str]:
        """Address to the Hub for monitoring.
        """
        return self._hub_address

    @hub_address.setter
    def hub_address(self, value: Optional[str]) -> None:
        self._hub_address = value

    @property
    def hub_port(self) -> Optional[int]:
        """Port to the Hub for monitoring.
        """
        return self._hub_port

    @hub_port.setter
    def hub_port(self, value: Optional[int]) -> None:
        self._hub_port = value


class HasConnectedWorkers():
    """A marker type to indicate that the executor has a count of connected workers"""
    connected_workers: int
    workers_per_node: float
