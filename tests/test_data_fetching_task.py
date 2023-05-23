import json
from multiprocessing import Queue
from pathlib import Path
from unittest import mock

import pytest

import utils
from tasks import DataFetchingTask

CITY_404 = {
    "GIZA": "https://code.s3.yandex.net/async-module/giza-response.json",
}
ERR_MSG_404 = "HTTP Error 404: Not Found"

@pytest.fixture(scope="module", autouse=True)
def mock_cities(cities):
    with mock.patch.object(utils, "CITIES", {**cities, **CITY_404}) as c:
        yield c

@pytest.fixture(scope="class", autouse=True)
def mock_get_forcasting(fetching_task_output_dir: Path):
    with mock.patch(
            "external.client.YandexWeatherAPI.get_forecasting"
    ) as get_forecasting:
        get_forecasting.side_effect = \
            lambda url: read_expected_output_by_url(
                output_dit=fetching_task_output_dir, url=url
            )
        yield get_forecasting

def read_expected_output_by_url(output_dit: Path, url: str) -> str:
    city_name = url.split("/")[-1].split("-")[0].upper()
    if city_name in CITY_404:
        raise Exception(ERR_MSG_404)

    forecast_file_path = output_dit / f"{city_name}.json"
    with open(forecast_file_path, "r") as f:
        forecast = json.load(f)
    return forecast


@pytest.fixture(scope="function")
def fetching_task(tmp_path, prepare_task) -> DataFetchingTask:
    return prepare_task(task_type=DataFetchingTask, tmp_dir=tmp_path)


class TestDataFetchingTask:
    @pytest.mark.smoke
    def test_call__404_exception(self, fetching_task, mock_logger):
        task = fetching_task.task

        input_queue = Queue()
        city_name = list(CITY_404.keys())[0]
        input_queue.put(city_name)
        task.input_queue = input_queue

        task()
        task.on_complete_event.wait()

        assert mock_logger.error.assert_called_once
        actual_err_message = str(mock_logger.error.call_args)
        assert ERR_MSG_404 in actual_err_message

    @pytest.mark.smoke
    def test_call__unknown_city_name(self, fetching_task, mock_logger):
        task = fetching_task.task

        input_queue = Queue()
        city_name = "UNKNOWN"
        input_queue.put(city_name)
        task.input_queue = input_queue

        task()
        task.on_complete_event.wait()

        assert mock_logger.error.assert_called_once
        actual_err_message = str(mock_logger.error.call_args)
        assert f"Unable to get forecast url." in actual_err_message
