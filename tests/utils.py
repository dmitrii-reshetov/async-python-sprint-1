from multiprocessing import Queue
from queue import Empty
from typing import NamedTuple

from tasks import AbstractTask


class DummyTask(AbstractTask):
    def _run(self, input_item: str):
        self.output_queue.put(input_item)


class PreparedTask(NamedTuple):
    task: AbstractTask
    input_queue_content: list[str]
    exp_output_queue_content: list[str]
    exp_output_files_content: set[str]


def read_queue_content(q: Queue) -> list[str]:
    result = list()

    try:
        while item := q.get(timeout=0.3):
            result.append(item)
    except Empty:
        pass

    return result
