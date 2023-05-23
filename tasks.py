import json
import subprocess
import sys
import time
from abc import ABC, abstractmethod
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
from copy import copy
from datetime import datetime
from multiprocessing import Queue
from pathlib import Path
from queue import Empty
from threading import Event
from typing import Iterable, Optional, TypedDict

from exceptions import (DataAggregationException, DataCalculationException,
                        DataFetchingException, ExpectedException)
from external.client import YandexWeatherAPI
from logger import logger
from settings import CALCULATOR_SCRIPT_PATH, DATA_DIR
from utils import CITIES, get_url_by_city_name


class AbstractTask(ABC):
    def __init__(
        self,
        input_queue: Queue,
        on_input_complete_event: Event,
        output_dir: Path,
        output_queue: Optional[Queue] = None,
    ):
        if output_queue is None:
            output_queue = Queue()

        self.input_queue = input_queue
        self.on_input_complete_event = on_input_complete_event
        self.output_queue = output_queue
        self.output_dir = output_dir
        self._on_complete_event = Event()

    @property
    def output_dir(self) -> Path:
        return self._output_dir

    @output_dir.setter
    def output_dir(self, value: Path):
        self._output_dir = value
        self._output_dir.mkdir(parents=False, exist_ok=True)

    @property
    def on_complete_event(self) -> Event:
        return self._on_complete_event

    def __call__(self):
        results = dict()
        with ThreadPoolExecutor(
                thread_name_prefix=self.__class__.__name__
        ) as pool:
            while True:
                try:
                    input_item = self.input_queue.get(timeout=0.3)
                    results[input_item] = pool.submit(
                        self._run, input_item
                    )
                except Empty:
                    if self.on_input_complete_event.is_set():
                        break
                    else:
                        continue

        self._after_threed_pool_exit(results)

    def _after_threed_pool_exit(self, results: dict[str, Future]):
        self.on_input_complete_event.clear()
        self._on_complete_event.set()
        self._process_results(results)

    @classmethod
    def _process_results(cls, results: dict[str, Future]) -> list:
        success_results = list()
        for input_item, result in results.items():
            try:
                success_results.append(result.result())
            except ExpectedException:
                continue
            except Exception:
                logger.error(
                    f"Unexpected error while running {cls.__name__}"
                    f" for {input_item}.", exc_info=True
                )

        total_co = len(results)
        success_co = len(success_results)
        logger.info(
            f"All input items have been processed by {cls.__name__}:"
            f" total: {total_co}, success: {success_co},"
            f" error: {total_co - success_co}."
        )
        return success_results

    @staticmethod
    def _dump(data: dict | list[dict], file_path: Path):
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)

    @abstractmethod
    def _run(self, input_item: str):
        pass


class DataFetchingTask(AbstractTask):
    def _run(self, input_item: str) -> None:
        city_name = input_item
        try:
            forecast_url = get_url_by_city_name(city_name)
        except Exception as e:
            err_message = f"{city_name}: Unable to get forecast url." \
                          f" Details: {e}"
            logger.error(err_message)
            raise DataFetchingException(err_message) from e

        try:
            forecast_data = YandexWeatherAPI.get_forecasting(forecast_url)
        except Exception as e:
            err_message = f"{city_name}: Unable to get forecast" \
                          f" by url: '{forecast_url}'. Details: {e}"
            logger.error(err_message)
            raise DataFetchingException(err_message) from e

        file_path = self._output_dir / f"{city_name}.json"
        self._dump(data=forecast_data, file_path=file_path)
        self.output_queue.put(str(file_path))

        logger.info(
            f"{city_name}: forecast data was successfully fetched"
            f" and saved as {file_path}"
        )


class DataCalculationTask(AbstractTask):
    def _run(self, input_item: str):
        forecast_file_path = Path(input_item)
        forecast_file_name = forecast_file_path.name
        output_file_path = self.output_dir / forecast_file_name
        command = [
            f"{sys.executable}",
            f"{CALCULATOR_SCRIPT_PATH}",
            "-i", f"{forecast_file_path}",
            "-o", f"{output_file_path}",
        ]
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        _, error = process.communicate()

        error_message = error.decode().strip()
        if error_message:
            error_message = f"Unable to calculate data from forecast" \
                            f" {forecast_file_path}. Details: {error_message}"
            if any(ext in error_message for ext in ("ERROR", "CRITICAL")):
                logger.error(error_message)
                raise DataCalculationException(error_message)
            else:
                logger.warning(error_message)

        self.output_queue.put(str(output_file_path))
        logger.info(
            f"{forecast_file_path}: forecast data was successfully calculated"
            f" and saved as {output_file_path}"
        )


class AggregatedData(TypedDict):
    city_name: str
    temperature: dict[str, float | None]
    precipitation_free_hours: dict[str, float | None]
    avg_temperature: float | None
    avg_precipitation_free_hours: float | None
    rating: int | None


class DataAggregationTask(AbstractTask):
    DELIMITER = ";"

    def _after_threed_pool_exit(self, results: dict[str, Future]):
        self.on_input_complete_event.clear()
        items = self._process_results(results)
        items = sorted(items, key=lambda i: i["city_name"])

        output_file_path = self.output_dir / "aggregated_data.json"
        self._dump(data=items, file_path=output_file_path)
        self.output_queue.put(str(output_file_path))
        self._on_complete_event.set()

    def _run(self, input_item: str) -> AggregatedData:
        input_file_path = Path(input_item)
        city_name = input_file_path.stem
        with open(input_file_path, "r") as f:
            data = json.load(f)

        try:
            days = data["days"]
        except KeyError:
            err_message = f"{city_name}: There is no data to aggregate" \
                          f" in {input_file_path}."
            logger.error(err_message)
            raise DataAggregationException(err_message)

        result: AggregatedData = dict(
            city_name=city_name,
            temperature=dict(),
            precipitation_free_hours=dict(),
            avg_temperature=None,
            avg_precipitation_free_hours=None,
            rating=None,
        )

        for day in days:
            date_object = datetime.strptime(day["date"], "%Y-%m-%d")
            short_date = date_object.strftime("%d-%m")

            result["temperature"][short_date] = day["temp_avg"]
            if day["temp_avg"] is None:
                logger.warning(
                    f"{city_name}: The average temperature is not specified"
                    f" on {short_date}."
                )

            result["precipitation_free_hours"][short_date] = \
                day["relevant_cond_hours"]
            if day["relevant_cond_hours"] is None:
                logger.warning(
                    f"{city_name}: The number of precipitation-free hours"
                    f" is not specified on {short_date}."
                )

        def _avg(values: Iterable) -> float:
            values = list(filter(lambda item: item is not None, values))
            if len(values) == 0:
                raise AttributeError("There is no one not None value.")
            return sum(values) / len(values)

        try:
            result["avg_temperature"] = _avg(result["temperature"].values())
        except AttributeError as e:
            logger.warning(
                f"{city_name}: Unable to calculate average temperature."
                f" Details: {e}"
            )

        try:
            result["avg_precipitation_free_hours"] = _avg(
                result["precipitation_free_hours"].values()
            )
        except AttributeError as e:
            logger.warning(
                f"{city_name}: Unable to calculate average number of"
                f" precipitation-free hours. Details: {e}"
            )

        return result


class DataAnalyzingTask(AbstractTask):
    def _run(self, input_item: str):
        input_file_path = Path(input_item)
        with open(input_file_path, "r") as input_file:
            items = json.load(input_file)

        rating_by_city_name = self._calc_multiple_fild_rating(
            items=items,
            field_names=("avg_temperature", "avg_precipitation_free_hours")
        )

        for item in items:
            city_rating = rating_by_city_name[item["city_name"]]
            item["rating"] = city_rating

        items = sorted(items, key=lambda i: i["city_name"])
        output_file_path = self.output_dir / "analyzed_data.json"
        self._dump(data=items, file_path=output_file_path)
        self.output_queue.put(str(output_file_path))

    def _calc_multiple_fild_rating(
            self,
            items: list[dict],
            field_names: Iterable[str]
    ) -> dict:
        ratings = dict()
        for field_name in field_names:
            ratings[field_name] = self._calc_single_field_rating(
                items=items,
                field_name=field_name
            )

        counter: Counter = Counter()
        for d in ratings.values():
            counter.update(d)

        sum_by_city_name = dict(counter)
        ordered_sums = sorted(list(set(sum_by_city_name.values())))
        result_rating_by_sum = {
            ratings_sum: position + 1
            for position, ratings_sum in enumerate(ordered_sums)
        }

        result_rating_by_city_name = dict()
        for city_name, rating_sum in sum_by_city_name.items():
            result_rating_by_city_name[city_name] = \
                result_rating_by_sum[rating_sum]

        return result_rating_by_city_name

    @staticmethod
    def _calc_single_field_rating(items: list[dict], field_name: str) -> dict:
        ordered_items = sorted(
            copy(items),
            key=lambda item: item[field_name],
            reverse=True
        )

        rating = {
            item["city_name"]: i + 1
            for i, item in enumerate(ordered_items)
        }

        return rating


class Utils:
    @staticmethod
    def mk_dir(
        parent_dir: Path, dir_name: str, add_timestamp: bool = True
    ) -> Path:
        if add_timestamp:
            timestamp = int(time.mktime(datetime.today().timetuple()))
            dir_name += f"_{timestamp}"
        tmp_dir = parent_dir / dir_name
        tmp_dir.mkdir(parents=False, exist_ok=True)
        return tmp_dir

    @staticmethod
    def _translate(eng_city_name: str) -> str:
        ru_name_by_eng = {
            "MOSCOW": "Москва",
            "PARIS": "Париж",
            "LONDON": "Лондон",
            "BERLIN": "Берлин",
            "BEIJING": "Пекин",
            "KAZAN": "Казань",
            "SPETERSBURG": "Санкт-Петербург",
            "VOLGOGRAD": "Волгоград",
            "NOVOSIBIRSK": "Новосибирск",
            "KALININGRAD": "Калининград",
            "ABUDHABI": "Абу-Даби",
            "WARSZAWA": "Варшава",
            "BUCHAREST": "Бухарест",
            "ROMA": "Рим",
            "CAIRO": "Каир",
            "GIZA": "Гиза",
            "MADRID": "Мадрид",
            "TORONTO": "Торонто",
        }

        result = ru_name_by_eng.get(eng_city_name.upper(), eng_city_name)
        if result == eng_city_name:
            logger.warning(
                f'Russian translation not found for "{eng_city_name}"'
            )

        return result

    @classmethod
    def print_result(cls, items: list[AggregatedData]):
        if not items:
            return

        prepared_rows = list()

        def prepare_val(val: float | int | None) -> str:
            number_format = "{:.1f}" if isinstance(val, float) else "{}"
            return "—" if val is None else number_format.format(val)

        for item in items:
            temperatures = map(prepare_val, item["temperature"].values())
            prepared_rows.append((
                cls._translate(item["city_name"]),
                "Температура, среднее",
                *temperatures,
                prepare_val(item["avg_temperature"]),
                prepare_val(item["rating"]),
            ))
            hours = map(prepare_val, item["precipitation_free_hours"].values())
            prepared_rows.append((
                "",
                "Без осадков, часов",
                *hours,
                prepare_val(item["avg_precipitation_free_hours"]),
                "",
            ))

        dates = items[0]["temperature"].keys()
        headers = (
            "Город/день", "", *dates, "Среднее", "Рейтинг"
        )
        row_format = "{:<15} {:<25}" + " {:<8}" * len(dates) + " {:<10} {:<10}"
        print(row_format.format(*headers))

        for row in prepared_rows:
            print(row_format.format(*row))


def main(cities: dict[str, str]):
    cities_queue: Queue = Queue()
    for city_name in cities.keys():
        cities_queue.put(city_name)
    on_input_complete_event = Event()
    on_input_complete_event.set()

    output_dir = Utils.mk_dir(parent_dir=DATA_DIR, dir_name="output")

    tasks: list[AbstractTask] = list()
    df_task = DataFetchingTask(
        input_queue=cities_queue,
        on_input_complete_event=on_input_complete_event,
        output_dir=output_dir / "fetched_data",
    )
    tasks.append(df_task)

    dc_task = DataCalculationTask(
        input_queue=df_task.output_queue,
        on_input_complete_event=df_task.on_complete_event,
        output_dir=output_dir / "calculated_data"
    )
    tasks.append(dc_task)

    dag_task = DataAggregationTask(
        input_queue=dc_task.output_queue,
        on_input_complete_event=dc_task.on_complete_event,
        output_dir=output_dir
    )
    tasks.append(dag_task)

    dan_task = DataAnalyzingTask(
        input_queue=dag_task.output_queue,
        on_input_complete_event=dag_task.on_complete_event,
        output_dir=output_dir
    )
    tasks.append(dan_task)

    results = dict()
    with ThreadPoolExecutor(thread_name_prefix="MainPool") as th_pool:
        for task in tasks:
            results[task.__class__.__name__] = th_pool.submit(task)

    dan_task.on_complete_event.wait()
    analyzed_data_path = dan_task.output_queue.get()
    with open(analyzed_data_path, "r") as f:
        items = json.load(f)

    top_rating_items = list(filter(lambda x: x["rating"] == 1, items))

    Utils.print_result(top_rating_items)


if __name__ == "__main__":
    main(CITIES)
