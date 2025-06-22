"""Progress tracking for long-running operations."""

import os
import sys
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any

from .logging import get_logger

logger = get_logger(__name__)


class ProgressTracker:
    """Track and display progress for long-running operations."""

    def __init__(
        self,
        total_items: int,
        description: str = "Processing",
        show_progress: bool | None = None,
        update_interval: float = 0.1,
    ):
        """
        Initialize progress tracker.

        Args:
            total_items: Total number of items to process
            description: Description of the operation
            show_progress: Whether to show progress (None = auto-detect)
            update_interval: Minimum time between updates (seconds)
        """
        self.total_items = total_items
        self.description = description
        self.current_item = 0
        self.start_time: float | None = None
        self.last_update_time: float = 0
        self.update_interval = update_interval
        self._lock = threading.Lock()

        # Determine if we should show progress
        if show_progress is None:
            # Auto-detect based on environment
            self.show_progress = self._should_show_progress()
        else:
            self.show_progress = show_progress

        # Determine progress style
        self.use_progress_bar = self._can_use_progress_bar()

    def _should_show_progress(self) -> bool:
        """Auto-detect whether to show progress."""
        # Check environment variable
        env_progress = os.environ.get("PYSPARK_ANALYZER_PROGRESS", "auto").lower()
        if env_progress == "never":
            return False
        if env_progress == "always":
            return True

        # Auto mode - check if we're in an interactive terminal
        return sys.stdout.isatty() and self.total_items > 1

    def _can_use_progress_bar(self) -> bool:
        """Check if we can use a progress bar."""
        if not self.show_progress:
            return False

        # Check if stdout is a terminal
        if not sys.stdout.isatty():
            return False

        # Check terminal width
        try:
            columns = os.get_terminal_size().columns
            return columns >= 60
        except (AttributeError, OSError):
            return False

    def start(self) -> None:
        """Start tracking progress."""
        self.start_time = time.time()
        self.current_item = 0
        self.last_update_time = 0

        if self.show_progress:
            if self.use_progress_bar:
                self._clear_line()
            else:
                logger.info(f"Starting {self.description}...")

    def update(self, item_name: str | None = None, increment: int = 1) -> None:
        """Update progress."""
        with self._lock:
            self.current_item += increment
            current_time = time.time()

            # Only update display if enough time has passed
            if current_time - self.last_update_time < self.update_interval:
                return

            self.last_update_time = float(current_time)

            if self.show_progress:
                if self.use_progress_bar:
                    self._display_progress_bar(item_name)
                else:
                    self._display_log_progress(item_name)

    def finish(self) -> None:
        """Finish tracking and display final message."""
        if self.show_progress and self.start_time is not None:
            elapsed_time = time.time() - self.start_time

            if self.use_progress_bar:
                self._clear_line()
                print(f"✓ {self.description} completed in {elapsed_time:.1f}s")
            else:
                logger.info(f"Completed {self.description} in {elapsed_time:.1f}s")

    def _display_progress_bar(self, item_name: str | None = None) -> None:
        """Display a progress bar in the terminal."""
        if self.total_items == 0:
            return

        # Calculate progress
        progress = self.current_item / self.total_items
        percentage = int(progress * 100)

        # Calculate elapsed and estimated time
        if self.start_time is None:
            return
        elapsed = time.time() - self.start_time
        if self.current_item > 0:
            eta = (elapsed / self.current_item) * (self.total_items - self.current_item)
            eta_str = f"ETA: {eta:.0f}s"
        else:
            eta_str = "ETA: calculating..."

        # Build progress bar
        bar_width = 30
        filled = int(bar_width * progress)
        bar = "█" * filled + "░" * (bar_width - filled)

        # Build status line
        status = f"{self.description}: [{bar}] {percentage}%"
        if item_name:
            status += f" | {item_name}"
        status += f" | {self.current_item}/{self.total_items} | {eta_str}"

        # Print with carriage return
        print(f"\r{status}", end="", flush=True)

    def _display_log_progress(self, item_name: str | None = None) -> None:
        """Display progress using logging."""
        if self.total_items == 0:
            return

        progress = self.current_item / self.total_items
        percentage = int(progress * 100)

        message = f"Progress: {percentage}% ({self.current_item}/{self.total_items})"
        if item_name:
            message += f" - {item_name}"

        logger.info(message)

    def _clear_line(self) -> None:
        """Clear the current line."""
        if sys.stdout.isatty():
            print("\r" + " " * 80 + "\r", end="", flush=True)


@contextmanager
def track_progress(
    total_items: int,
    description: str = "Processing",
    show_progress: bool | None = None,
    callback: Callable[[ProgressTracker], Any] | None = None,
) -> Any:
    """
    Context manager for progress tracking.

    Args:
        total_items: Total number of items to process
        description: Description of the operation
        show_progress: Whether to show progress (None = auto-detect)
        callback: Optional callback to receive progress tracker

    Yields:
        ProgressTracker instance

    Example:
        with track_progress(len(columns), "Analyzing columns") as tracker:
            for col in columns:
                tracker.update(col)
                # Process column
    """
    tracker = ProgressTracker(total_items, description, show_progress)
    tracker.start()

    try:
        if callback:
            callback(tracker)
        yield tracker
    finally:
        tracker.finish()


class ProgressStage:
    """Track multi-stage operations with sub-progress."""

    def __init__(
        self, stages: list[tuple[str, int]], show_progress: bool | None = None
    ):
        """
        Initialize multi-stage progress tracker.

        Args:
            stages: List of (stage_name, estimated_weight) tuples
            show_progress: Whether to show progress
        """
        self.stages = stages
        self.total_weight = sum(weight for _, weight in stages)
        self.current_stage_idx = 0
        self.current_stage_progress = 0
        self.show_progress = show_progress
        self._overall_tracker: ProgressTracker | None = None
        self._stage_tracker: ProgressTracker | None = None

    def start(self) -> None:
        """Start tracking progress."""
        if self.show_progress and self.stages:
            self._overall_tracker = ProgressTracker(
                self.total_weight, "Overall progress", self.show_progress
            )
            self._overall_tracker.start()
            self._start_stage(0)

    def next_stage(self) -> ProgressTracker | None:
        """Move to the next stage and return its tracker."""
        if self.current_stage_idx < len(self.stages) - 1:
            # Complete current stage
            if self._stage_tracker:
                self._stage_tracker.finish()

            # Update overall progress
            if self._overall_tracker:
                stage_name, weight = self.stages[self.current_stage_idx]
                self._overall_tracker.update(f"Completed {stage_name}", weight)

            # Start next stage
            self.current_stage_idx += 1
            self._start_stage(self.current_stage_idx)
            return self._stage_tracker
        return None

    def _start_stage(self, idx: int) -> None:
        """Start a specific stage."""
        if idx < len(self.stages):
            stage_name, weight = self.stages[idx]
            if self.show_progress:
                # For now, we'll use weight as the number of items
                # This could be enhanced to support actual item counts per stage
                self._stage_tracker = ProgressTracker(
                    weight,
                    f"Stage {idx + 1}/{len(self.stages)}: {stage_name}",
                    self.show_progress,
                )
                self._stage_tracker.start()

    def finish(self) -> None:
        """Finish all tracking."""
        if self._stage_tracker:
            self._stage_tracker.finish()
        if self._overall_tracker:
            self._overall_tracker.finish()
