import json
from pathlib import Path

import pytest

from tests.utils import PreparedTask, read_queue_content


class TestCommon:
    @pytest.mark.smoke
    def test_call__output_files_exists(self, prepared_task: PreparedTask):
        task = prepared_task.task
        assert not task.input_queue.empty()

        task()
        task.on_complete_event.wait()

        output_file_paths = read_queue_content(task.output_queue)
        assert len(output_file_paths) > 0
        assert all(Path(fp).exists() for fp in output_file_paths)


    @pytest.mark.smoke
    def test_call__output_queue_content(self, prepared_task: PreparedTask):
        task = prepared_task.task

        task()
        task.on_complete_event.wait()

        actual_content = set(read_queue_content(task.output_queue))
        assert actual_content == prepared_task.exp_output_queue_content


    @pytest.mark.smoke
    def test_call__output_file_content(self, prepared_task: PreparedTask):
        task = prepared_task.task

        task()
        task.on_complete_event.wait()

        output_file_paths = read_queue_content(task.output_queue)
        actual_files_content = set()
        for file_path in output_file_paths:
            with open(file_path, "r") as f:
                actual_files_content.add(f.read())

        assert actual_files_content == prepared_task.exp_output_files_content
