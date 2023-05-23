from multiprocessing import Queue

import pytest

from tasks import DataAggregationTask


@pytest.fixture(scope="function")
def aggregation_task(tmp_path, prepare_task) -> DataAggregationTask:
    return prepare_task(task_type=DataAggregationTask, tmp_dir=tmp_path)


class TestDataCalculationTask:
    @pytest.mark.smoke
    def test_call__empty_input_file(
            self, empty_json_path, aggregation_task, mock_logger
    ):
        task = aggregation_task.task

        input_queue = Queue()
        input_queue.put(empty_json_path)
        task.input_queue = input_queue

        task()

        assert mock_logger.error.assert_called_once
        actual_err_message = str(mock_logger.error.call_args)
        assert "There is no data to aggregate" in actual_err_message


