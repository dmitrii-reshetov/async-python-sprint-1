import json
from unittest import mock
from typing import Callable
from pathlib import Path
from multiprocessing import Event, Queue

import pytest

from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from tests.utils import DummyTask, PreparedTask

CITIES = {
    "MOSCOW": "https://code.s3.yandex.net/async-module/moscow-response.json",
    "PARIS": "https://code.s3.yandex.net/async-module/paris-response.json",
}

PROJECT_ROOT_DIR = Path().parent.parent.resolve()
TEST_DATA_DIR = PROJECT_ROOT_DIR / "tests" / "data"
EXPECTED_OUTPUT_PATH = {
    DataFetchingTask: TEST_DATA_DIR / "data_fetching_task_output",
    DataCalculationTask: TEST_DATA_DIR / "data_calculation_task_output",
    DataAggregationTask: TEST_DATA_DIR / "data_aggregation_task_output.json",
    DataAnalyzingTask: TEST_DATA_DIR / "data_analyzing_task_output.json",
}

@pytest.fixture(scope="session")
def fetching_task_output_dir() -> Path:
    return EXPECTED_OUTPUT_PATH[DataFetchingTask]

INPUT_Q_CONTENT: dict[type, list[str]] = {
    DummyTask: [f"dummy_item_{i}" for i in range(2)],
    DataFetchingTask: CITIES.keys(),
    DataCalculationTask: [
        str(fp) for fp in EXPECTED_OUTPUT_PATH[DataFetchingTask].glob("*")
    ],
    DataAggregationTask: [
        str(fp) for fp in EXPECTED_OUTPUT_PATH[DataCalculationTask].glob("*")
    ],
    DataAnalyzingTask: [str(EXPECTED_OUTPUT_PATH[DataAggregationTask])],
}

def get_exp_output_q_content(
        task_type: type,
        output_dir: Path | None = None
) -> set[str]:
    match task_type.__name__:
        case DummyTask.__name__:
            result = set(INPUT_Q_CONTENT[DummyTask])
        case DataFetchingTask.__name__:
            result = {
                str(output_dir / f"{city_name}.json")
                for city_name in INPUT_Q_CONTENT[DataFetchingTask]
            }
        case DataCalculationTask.__name__:
            result = {
                    str(output_dir / Path(file_path).name)
                    for file_path in INPUT_Q_CONTENT[DataCalculationTask]
            }
        case DataAggregationTask.__name__:
            result = { str(output_dir / "aggregated_data.json"), }
        case DataAnalyzingTask.__name__:
            result = { str(output_dir / "analyzed_data.json"), }

    return result


def get_exp_output_files_content(task_type: type) -> set[str]:
    match task_type.__name__:
        case DummyTask.__name__:
            file_paths = list()
        case DataFetchingTask.__name__ | DataCalculationTask.__name__:
            file_paths = EXPECTED_OUTPUT_PATH[task_type].glob("*")
        case DataAggregationTask.__name__ | DataAnalyzingTask.__name__:
            file_paths = [EXPECTED_OUTPUT_PATH[task_type]]

    result = set()
    for file_path in file_paths:
        with open(file_path, "r") as f:
            result.add(f.read())
    return result


def _prepare_task(task_type: type, tmp_dir: Path) -> PreparedTask:
    input_queue = Queue()
    input_queue_content = set(INPUT_Q_CONTENT[task_type])
    for item in input_queue_content:
        input_queue.put(item)

    on_input_complete_event = Event()
    on_input_complete_event.set()
    output_dir = tmp_dir / f"{task_type.__name__}_output_dir"
    task = task_type(
        input_queue=input_queue,
        on_input_complete_event=on_input_complete_event,
        output_dir=output_dir,
    )

    return PreparedTask(
        task=task,
        input_queue_content=input_queue_content,
        exp_output_queue_content=get_exp_output_q_content(
            task_type=task_type, output_dir=output_dir
        ),
        exp_output_files_content=get_exp_output_files_content(task_type)
    )

@pytest.fixture(scope="session")
def prepare_task() -> Callable:
    return _prepare_task


@pytest.fixture(scope="function")
def fetching_task(tmp_path) -> DataFetchingTask:
    return prepare_task(task_type=DataFetchingTask, tmp_dir=tmp_path)

@pytest.fixture(
    scope="function",
    params=[
        DataFetchingTask,
        DataCalculationTask,
        DataAggregationTask,
        DataAnalyzingTask,
    ]
)
def prepared_task(request, tmp_path):
    return _prepare_task(task_type=request.param, tmp_dir=tmp_path)

@pytest.fixture(scope="session")
def cities():
    return CITIES


@pytest.fixture(scope="session")
def mock_logger():
    import tasks
    with mock.patch.object(tasks, "logger") as logger:
        yield logger

@pytest.fixture(scope="function")
def empty_json_path(tmp_path: Path):
    file_path = tmp_path / "empty.json"
    with open(file_path, "w") as fp:
        json.dump(dict(), fp)
    return file_path
