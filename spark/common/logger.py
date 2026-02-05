"""
Shared logger for Spark jobs: console + optional file output for debugging.
Use for Bronze/Silver jobs so errors and info are written to logs/*.txt (exam requirement).
"""
import logging
import os
from typing import Optional


LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logger(
    name: str,
    level: int = logging.INFO,
    log_dir: Optional[str] = None,
    log_file_basename: Optional[str] = None,
):
    """
    Create a logger that prints to console and optionally appends to a .txt file.

    Args:
        name: Logger name (e.g. __name__ or "feeder_csv").
        level: Logging level (default INFO).
        log_dir: Directory for log file (e.g. "logs/bronze"). If None, no file.
        log_file_basename: Base name for log file (e.g. "feeder_csv" -> feeder_csv.txt).

    Returns:
        Logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding handlers again if this logger was already configured (e.g. in tests)
    if logger.handlers:
        return logger

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # Console
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File (for debugging and exam: "logs exported to .txt file per run")
    if log_dir and log_file_basename:
        try:
            os.makedirs(log_dir, exist_ok=True)
            log_path = os.path.join(log_dir, f"{log_file_basename}.txt")
            fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
            fh.setLevel(level)
            fh.setFormatter(formatter)
            logger.addHandler(fh)
            logger.info("Log file: %s", os.path.abspath(log_path))
        except OSError as e:
            logger.warning("Could not create log file %s: %s", log_dir, e)

    return logger
