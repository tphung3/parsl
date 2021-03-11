import logging
import parsl  # noqa F401 (used in string type annotation)
import time
import zmq
from typing import Any, cast, Dict, Sequence, Optional
from typing import List  # noqa F401 (used in type annotation)

from parsl.dataflow.executor_status import ExecutorStatus
from parsl.dataflow.job_error_handler import JobErrorHandler
from parsl.dataflow.strategy import Strategy
from parsl.executors.base import ParslExecutor
from parsl.monitoring.message_type import MessageType

from parsl.providers.provider_base import JobStatus, JobState

logger = logging.getLogger(__name__)


class PollItem(ExecutorStatus):
    def __init__(self, executor: ParslExecutor, dfk: "parsl.dataflow.dflow.DataFlowKernel"):
        self._executor = executor
        self._dfk = dfk
        self._interval = executor.status_polling_interval
        self._last_poll_time = 0.0
        self._status = {}  # type: Dict[str, JobStatus]

        # Create a ZMQ channel to send poll status to monitoring
        self.monitoring_enabled = False

        # mypy 0.790 cannot determine the type for self._dfk.monitoring
        # even though it can determine that _dfk is a DFK. Perhaps because of
        # the same cyclic import that makes DataFlowKernel need to be quoted
        # in the __init__ type signature?
        # So explicitly ignore this type problem.
        if self._dfk.monitoring is not None:  # type: ignore
            self.monitoring_enabled = True
            hub_address = self._dfk.hub_address
            hub_port = self._dfk.hub_interchange_port
            context = zmq.Context()
            self.hub_channel = context.socket(zmq.DEALER)
            self.hub_channel.set_hwm(0)
            self.hub_channel.connect("tcp://{}:{}".format(hub_address, hub_port))
            logger.info("Monitoring enabled on task status poller")

    def _should_poll(self, now: float) -> bool:
        return now >= self._last_poll_time + self._interval

    def poll(self, now: float) -> None:
        if self._should_poll(now):
            self._status = self._executor.status()
            self._last_poll_time = now
            self.send_monitoring_info(self._status)

    # status: isn't optional so shouldn't default to None. I think?
    # can I make the dict type more specific?
    def send_monitoring_info(self, status: Dict) -> None:
        # Send monitoring info for HTEX when monitoring enabled
        if self.monitoring_enabled:
            msg = self._executor.create_monitoring_info(status)
            logger.debug("Sending message {} to hub from task status poller".format(msg))
            self.hub_channel.send_pyobj((MessageType.BLOCK_INFO, msg))

    @property
    def status(self) -> Dict[str, JobStatus]:
        """Return the status of all jobs/blocks of the executor of this poller.

        :return: a dictionary mapping block ids (in string) to job status
        """
        return self._status

    @property
    def executor(self) -> ParslExecutor:
        return self._executor

    def scale_in(self, n: int, force: bool = True, max_idletime: Optional[float] = None) -> List[str]:
        if force and not max_idletime:
            block_ids = self._executor.scale_in(n)
        else:
            # this cast is because ParslExecutor.scale_in doesn't have force or max_idletime parameters
            # so we just hope that the actual executor happens to have them.
            # see some notes in ParslExecutor about making the status handling superclass into a
            # class that holds all the scaling methods, so that everything can be specialised
            # to work on those.
            block_ids = cast(Any, self._executor).scale_in(n, force=force, max_idletime=max_idletime)
        if block_ids is not None:
            new_status = {}
            for block_id in block_ids:
                new_status[block_id] = JobStatus(JobState.CANCELLED)
                del self._status[block_id]
            self.send_monitoring_info(new_status)
        return block_ids

    def scale_out(self, n: int) -> List[str]:
        logger.debug("BENC: in task status scale out")
        block_ids = self._executor.scale_out(n)
        logger.debug("BENC: executor scale out has returned")

        if block_ids is not None:
            logger.debug(f"BENC: there were some block ids, {block_ids}, which will now be set to pending")
            new_status = {}
            for block_id in block_ids:
                new_status[block_id] = JobStatus(JobState.PENDING)
            self.send_monitoring_info(new_status)
            self._status.update(new_status)
        # else:
            # mypy declares this as unreachable...
            # logger.debug("BENC: there were no block IDs returned from the scale out call")
        return block_ids

    def __repr__(self) -> str:
        return self._status.__repr__()


class TaskStatusPoller(object):
    def __init__(self, dfk: "parsl.dataflow.dflow.DataFlowKernel"):
        self._poll_items = []  # type: List[PollItem]
        self.dfk = dfk
        self._strategy = Strategy(dfk)
        self._error_handler = JobErrorHandler()

    def poll(self, tasks: Optional[Sequence[str]] = None, kind: Optional[str] = None) -> None:
        self._update_state()

        # List is invariant, and the type of _poll_items if List[PollItem]
        # but run wants a list of ExecutorStatus.
        # This cast should be safe *if* .run does not break the reason that
        # List is invariant, which is that it does not add anything into the
        # the list (otherwise, List[PollItem] might end up with ExecutorStatus not-PollItems in it.
        self._error_handler.run(cast(List[ExecutorStatus], self._poll_items))
        self._strategy.strategize(self._poll_items, tasks)

    def _update_state(self) -> None:
        now = time.time()
        for item in self._poll_items:
            item.poll(now)

    def add_executors(self, executors: Sequence[ParslExecutor]) -> None:
        for executor in executors:
            if executor.status_polling_interval > 0:
                logger.debug("Adding executor {}".format(executor.label))
                self._poll_items.append(PollItem(executor, self.dfk))
        self._strategy.add_executors(executors)
