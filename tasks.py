import logging
import multiprocessing
import os
import sys
import json
import time
from copy import copy
from datetime import datetime
from pathlib import Path
from queue import Empty
from typing import Iterable
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from multiprocessing import Pool, Queue

from utils import CITIES, get_url_by_city_name
from external.client import YandexWeatherAPI

PROJECT_ROOT_DIR = Path().absolute()
DATA_DIR = PROJECT_ROOT_DIR / "data"
LOGS_DIR = PROJECT_ROOT_DIR / "logs"
DOWNLOADS_DIR = DATA_DIR / "downloads"
ANALYZER_OUTPUT_DIR = DATA_DIR / "analyzer_output"
ANALYZER_SCRIPT_PATH = PROJECT_ROOT_DIR / "external" / "analyzer.py"


logging.basicConfig(level=logging.CRITICAL)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False

format = '[%(asctime)s:%(name)s:%(levelname)s] %(message)s'

formatter = logging.Formatter(format)

f_handler = logging.FileHandler(LOGS_DIR / 'main.log')
f_handler.setFormatter(formatter)
f_handler.setLevel(logging.DEBUG)

s_handler = logging.StreamHandler()
s_handler.setFormatter(formatter)
s_handler.setLevel(logging.WARNING)

logger.addHandler(f_handler)
logger.addHandler(s_handler)

class DataFetchingTask:
    def __init__(
            self,
            input_queue: Queue,
            output_queue: Queue,
            output_dir: str | Path,
            on_complete_event: Event,
    ):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.output_dir = Path(output_dir)
        self.on_complete_event = on_complete_event


    def run(self):
        with ThreadPoolExecutor() as pool:
            while not self.input_queue.empty():
                city_name = self.input_queue.get()
                pool.submit(self._fetch_forcast_fo_city, city_name)
        self.on_complete_event.set()
        logger.debug("All forecasts have been fetched. ")

    def _fetch_forcast_fo_city(self, city_name: str):
        try:
            forecast_url = get_url_by_city_name(city_name)
        except KeyError:
            logger.error(f"{city_name}: Forecast url not found.")
            return

        try:
            forecast_data = YandexWeatherAPI.get_forecasting(forecast_url)
        except Exception as e:
            logger.error(
                f"{city_name}: Error while forecast data fetching."
                f" Forecast URL: {forecast_url}"
                f" Details: {e}"
            )
            return
        else:
            file_path = self.output_dir / f"{city_name}.json"
            with open(file_path, "w") as f:
                json.dump(forecast_data, f, indent=4)

            self.output_queue.put(file_path)
            logger.debug(
                f"{city_name}: forecast data was successfully fetched.")


class DataCalculationTask:
    def __init__(
        self,
        analyzer_script_path: str | Path,
        output_dir: str | Path,
        input_queue: Queue,
        stop_calculation_event: Event,
        on_complete_event: Event
    ):
        self._analyzer_script_path = Path(analyzer_script_path)
        self.input_queue = input_queue
        self.output_dir = Path(output_dir)
        self._stop_calculation_event = stop_calculation_event
        self.on_complete_event = on_complete_event

    def run(self):
        with Pool(processes=multiprocessing.cpu_count() - 1) as pool:
            while True:
                try:
                    forecast_file_path = self.input_queue.get(timeout=0.1)
                except Empty:
                    if self._stop_calculation_event.is_set():
                        break
                    else:
                        continue

                forecast_file_name = forecast_file_path.name
                command = '{python} {script} -i "{input}" -o "{output}"'.\
                    format(
                        python=sys.executable,
                        script=self._analyzer_script_path,
                        input=forecast_file_path,
                        output=self.output_dir / forecast_file_name,
                    )

                pool.apply_async(os.system, [command])
                logger.debug(
                    f"Calculation of {forecast_file_path} "
                    f"has been started asynchronously."
                )
        self.on_complete_event.set()
        logger.debug("All data has been calculated.")


class DataAggregationTask:

    def __init__(self, input_dir: str | Path, output_dir: str | Path):
        self._input_dir = Path(input_dir)
        self._output_dir = Path(output_dir)

    def run(self):
        analyzed_data_dir = Path(self._input_dir)

        with ThreadPoolExecutor() as pool:
            file_paths = self._input_dir.glob("*")
            items = pool.map(self._read_data, file_paths)

        items = [item for item in items if item]

        output_file_name = f"aggregated_{analyzed_data_dir.name}.json"
        output_file_path = self._output_dir / output_file_name
        with open(output_file_path, "w") as f:
            json.dump(items, f, indent=4)

        logger.debug(f"All data has ben aggregated: {output_file_path}")
        return output_file_path

    @staticmethod
    def _read_data(file_path: str | Path) -> dict | None:
        file_path = Path(file_path)
        city_name = file_path.stem
        with open(file_path, "r") as f:
            data = json.load(f)

        try:
            days = data["days"]
        except KeyError:
            return None

        result = dict(
            city_name=city_name,
            temperature=dict(),
            precipitation_free_hours=dict(),
            avg_temperature=None,
            avg_precipitation_free_hours=None,
            rating=None,
        )

        for day in days:
            date_object = datetime.strptime(day["date"], "%Y-%m-%d")
            short_date: str = date_object.strftime("%d-%m")
            result["temperature"][short_date] = day["temp_avg"]
            result["precipitation_free_hours"][short_date] = \
                day["relevant_cond_hours"]

        def avg(l: Iterable) -> float:
            values = [val for val in l if val is not None]
            return sum(values) / len(values)

        result["avg_temperature"] = avg(result["temperature"].values())
        result["avg_precipitation_free_hours"] = \
            avg(result["precipitation_free_hours"].values())
        return result


class DataAnalyzingTask:
    def __init__(self, input_file_path: str | Path):
        self.input_file_path = input_file_path

    def run(self) -> list[dict]:
        with open(self.input_file_path, "r") as input_file:
            items = json.load(input_file)

        rating_by_city_name = self._calc_multiple_fild_rating(
            items=items,
            field_names=("avg_temperature", "avg_precipitation_free_hours")
        )
        #Utils.print_result(items)
        result_items = list()
        for item in items:
            city_rating = rating_by_city_name[item["city_name"]]
            if city_rating == 1:
                item["rating"] = city_rating
                result_items.append(item)

        return result_items

    def _calc_multiple_fild_rating(
            self,
            items: list[dict],
            field_names: list[str]
    ) -> dict:
        ratings = dict()
        for field_name in field_names:
            ratings[field_name] = self._calc_single_field_rating(
                items=items,
                field_name=field_name
            )

        counter = Counter()
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
    def mk_dir(parent_dir: Path, dir_name: str, add_timestamp: bool=True) -> Path:
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
            logger.warning(f'Russian translation not found for "{eng_city_name}"')

        return result
    @classmethod
    def print_result(cls, items: list[dict]):
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
        dates = item["temperature"].keys()
        headers = (
            "Город/день", "", *dates, "Среднее", "Рейтинг"
        )
        row_format = "{:<15} {:<25}" + " {:<8}"*len(dates) + " {:<10} {:<10}"
        print(row_format.format(*headers))

        for row in prepared_rows:
            print(row_format.format(*row))


if __name__ == "__main__":

    with ThreadPoolExecutor() as th_pool:
        cities_queue = Queue()
        for city_name in CITIES.keys():
            cities_queue.put(city_name)

        # Forecasts fetching
        df_task = DataFetchingTask(
            input_queue=cities_queue,
            output_queue=Queue(),
            on_complete_event=Event(),
            output_dir=Utils.mk_dir(
                parent_dir=DOWNLOADS_DIR,
                dir_name="forecasts"
            ),
        )
        th_pool.submit(df_task.run)

        # Forecasts calculation
        dc_task = DataCalculationTask(
            analyzer_script_path=ANALYZER_SCRIPT_PATH,
            input_queue=df_task.output_queue,
            stop_calculation_event = df_task.on_complete_event,
            on_complete_event = Event(),
            output_dir = Utils.mk_dir(
                parent_dir=ANALYZER_OUTPUT_DIR,
                dir_name=df_task.output_dir.name,
                add_timestamp=False
            )
        )
        th_pool.submit(dc_task.run)


        # Data aggregation
        dag_task = DataAggregationTask(
            input_dir=dc_task.output_dir,
            output_dir=DOWNLOADS_DIR
        )
        dc_task.on_complete_event.wait()
        aggregated_data_path = dag_task.run()

    # Data analyzing
    dan_task = DataAnalyzingTask(
        input_file_path=aggregated_data_path,
    )
    result = dan_task.run()
    Utils.print_result(result)
