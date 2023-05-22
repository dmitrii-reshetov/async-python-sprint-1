
from multiprocessing import Queue

import pytest

from tasks import DataCalculationTask


@pytest.fixture(scope="function")
def calculation_task(tmp_path, prepare_task) -> DataCalculationTask:
    return prepare_task(task_type=DataCalculationTask, tmp_dir=tmp_path)


class TestDataCalculationTask:
    @pytest.mark.smoke
    def test_call__empty_input_file(
            self, empty_json_path, calculation_task, mock_logger
    ):
        task = calculation_task.task

        input_queue = Queue()
        input_queue.put(empty_json_path)
        task.input_queue = input_queue

        task()

        assert mock_logger.warning.assert_called_once
        actual_err_message = str(mock_logger.warning.call_args)
        assert "Input data is empty" in actual_err_message


