from multiprocessing import Event, Queue

import pytest

from tests.utils import DummyTask, PreparedTask


@pytest.fixture(scope="function")
def dummy_task(tmp_path, prepare_task) -> DummyTask:
    result = prepare_task(task_type=DummyTask, tmp_dir=tmp_path)
    return result


class TestAbstractTask:
    @pytest.mark.smoke
    def test_init__output_directory_created(self, tmp_path):
        output_dir = tmp_path / "dummy_dir"
        assert not output_dir.exists()
        _ = DummyTask(
            input_queue=Queue(),
            on_input_complete_event=Event(),
            output_dir=output_dir
        )

        assert output_dir.exists()

    @pytest.mark.smoke
    def tests_init__output_dir_not_exists_error(self, tmp_path):
        output_dir = tmp_path / "not_exist" / "dummy_dir"
        assert not output_dir.parent.exists()
        with pytest.raises(FileNotFoundError):
            _ = DummyTask(
                input_queue=Queue(),
                on_input_complete_event=Event(),
                output_dir=output_dir
            )

    @pytest.mark.smoke
    def test_call__on_complete_event_is_set_empty(
            self, dummy_task: PreparedTask
    ):
        task = dummy_task.task
        assert task.on_input_complete_event.is_set()
        assert not task._on_complete_event.is_set()

        task()
        task.on_complete_event.wait()

        assert not task.on_input_complete_event.is_set()

    @pytest.mark.smoke
    def test_call__on_complete_event_is_set_not_empty(
            self, dummy_task: PreparedTask
    ):
        task = dummy_task.task
        assert task.on_input_complete_event.is_set()
        assert not task.on_complete_event.is_set()

        task()
        task.on_complete_event.wait()

        assert not task.on_input_complete_event.is_set()

