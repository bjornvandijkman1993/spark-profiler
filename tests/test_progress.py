"""Tests for progress tracking functionality."""

import threading
import time
from io import StringIO
from unittest.mock import MagicMock, patch

from pyspark_analyzer.progress import ProgressStage, ProgressTracker, track_progress


class TestProgressTracker:
    """Test ProgressTracker class."""

    def test_init_default(self):
        """Test default initialization."""
        tracker = ProgressTracker(10, "Test")
        assert tracker.total_items == 10
        assert tracker.description == "Test"
        assert tracker.current_item == 0
        assert tracker.update_interval == 0.1

    def test_auto_detect_progress(self):
        """Test auto-detection of progress display."""
        with patch("sys.stdout.isatty", return_value=True):
            tracker = ProgressTracker(10)
            assert tracker.show_progress is True

        with patch("sys.stdout.isatty", return_value=False):
            tracker = ProgressTracker(10)
            assert tracker.show_progress is False

    def test_force_progress(self):
        """Test forcing progress on/off."""
        tracker = ProgressTracker(10, show_progress=True)
        assert tracker.show_progress is True

        tracker = ProgressTracker(10, show_progress=False)
        assert tracker.show_progress is False

    def test_environment_variable(self):
        """Test environment variable control."""
        import os

        # Test "never"
        with patch.dict(os.environ, {"PYSPARK_ANALYZER_PROGRESS": "never"}):
            tracker = ProgressTracker(10)
            assert tracker.show_progress is False

        # Test "always"
        with patch.dict(os.environ, {"PYSPARK_ANALYZER_PROGRESS": "always"}):
            tracker = ProgressTracker(10)
            assert tracker.show_progress is True

    def test_update(self):
        """Test progress updates."""
        tracker = ProgressTracker(10, show_progress=False)
        tracker.start()

        # Test single update
        tracker.update()
        assert tracker.current_item == 1

        # Test multiple updates
        tracker.update(increment=3)
        assert tracker.current_item == 4

    def test_thread_safety(self):
        """Test thread safety of updates."""
        tracker = ProgressTracker(100, show_progress=False)
        tracker.start()

        def update_progress():
            for _ in range(10):
                tracker.update()

        threads = [threading.Thread(target=update_progress) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert tracker.current_item == 100

    @patch("sys.stdout", new_callable=StringIO)
    def test_progress_bar_output(self, mock_stdout):
        """Test progress bar output."""
        with patch("sys.stdout.isatty", return_value=True), patch(
            "os.get_terminal_size", return_value=MagicMock(columns=80)
        ):
            tracker = ProgressTracker(10, "Testing", show_progress=True)
            tracker.use_progress_bar = True
            tracker.start()
            tracker.update("item1")

            output = mock_stdout.getvalue()
            assert "Testing:" in output
            assert "10%" in output
            assert "item1" in output

    def test_context_manager(self):
        """Test context manager usage."""
        with track_progress(5, "Test", show_progress=False) as tracker:
            assert tracker.total_items == 5
            tracker.update()
            assert tracker.current_item == 1

    def test_progress_stage(self):
        """Test multi-stage progress tracking."""
        stages = [("Stage 1", 10), ("Stage 2", 20), ("Stage 3", 30)]
        stage = ProgressStage(stages, show_progress=False)

        stage.start()
        assert stage.current_stage_idx == 0

        # Move to next stage
        tracker = stage.next_stage()
        assert stage.current_stage_idx == 1

        # Move to last stage
        tracker = stage.next_stage()
        assert stage.current_stage_idx == 2

        # No more stages
        tracker = stage.next_stage()
        assert tracker is None

    @patch("pyspark_analyzer.progress.logger")
    def test_log_progress(self, mock_logger):
        """Test logging-based progress."""
        tracker = ProgressTracker(10, "Test", show_progress=True)
        tracker.use_progress_bar = False
        tracker.start()

        # Check start message
        mock_logger.info.assert_called_with("Starting Test...")

        # Update and check progress message
        tracker.update("Column A")
        mock_logger.info.assert_called_with("Progress: 10% (1/10) - Column A")

    def test_eta_calculation(self):
        """Test ETA calculation."""
        tracker = ProgressTracker(100, show_progress=False)
        tracker.start()

        # Simulate some work
        time.sleep(0.1)
        tracker.update(increment=10)

        # ETA should be calculated based on elapsed time and remaining items
        assert tracker.current_item == 10
        assert tracker.start_time is not None

    def test_finish(self):
        """Test finish method."""
        with patch("pyspark_analyzer.progress.logger") as mock_logger:
            tracker = ProgressTracker(10, "Test", show_progress=True)
            tracker.use_progress_bar = False
            tracker.start()
            tracker.finish()

            # Check completion message
            assert any(
                "Completed Test" in str(call)
                for call in mock_logger.info.call_args_list
            )
