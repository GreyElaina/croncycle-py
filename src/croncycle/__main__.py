from __future__ import annotations
from datetime import datetime
import sys
import time

from loguru import logger
from humanize import naturaldelta, naturaldate
from typing_extensions import Annotated
from richuru import install as install_richuru
import subprocess
from croniter import croniter

import typer


def main(
    command: list[str],
    cron: Annotated[
        str, typer.Option("--cron", "-t", help="Cron expression to schedule the job")
    ],
    quiet: Annotated[
        bool, typer.Option("--quiet", "-q", help="Suppress output")
    ] = False,
    exit_on_error: Annotated[
        bool, typer.Option("--exit-on-error", "-e", help="Exit on error")
    ] = False,
    ignored_codes: Annotated[
        list[int],
        typer.Option(
            "--ignored-codes",
            "-c",
            help="Ignore these exit codes when --exit-on-error is enabled (comma separated)",
        ),
    ] = [],
    no_richuru: Annotated[
        bool, typer.Option("--no-color", help="Disable richuru")
    ] = False,
    stdin: Annotated[bool, typer.Option("--enable-stdin", "-i", help="Enable stdin, disabled by default")] = False,
    stderr_to_stdout: Annotated[
        bool,
        typer.Option("--stderr-to-stdout", "-r", help="Redirect command stderr to stdout"),
    ] = False,
    no_output: Annotated[
        bool, typer.Option("--no-output", help="Disable command output")
    ] = False,
) -> None:
    if quiet:
        logger.remove()
        logger.add(sys.stderr, level="ERROR")
    elif not no_richuru:
        install_richuru()

    logger.info(f"Running job with cron expression: {cron}")
    logger.info(f"Command: {' '.join(command)}")

    iter = croniter(cron, start_time=datetime.now())

    while True:
        next_run = iter.get_next(datetime)

        if next_run <= datetime.now():
            logger.warning(f"Next run is in the past: {next_run}")
            continue

        logger.info(
            f"Next run at {naturaldate(next_run)} ({naturaldelta(next_run - datetime.now())})"
        )

        secs = (next_run - datetime.now()).total_seconds()

        try:
            time.sleep(secs)
        except KeyboardInterrupt:
            logger.info("Exiting...")
            raise typer.Exit(0)

        logger.info("Running job...")
        logger.debug(f"Running command: {' '.join(command)}")

        try:
            result = subprocess.run(
                command,
                stdin=sys.stdin if stdin else subprocess.DEVNULL,
                stdout=sys.stdout if not no_output else subprocess.DEVNULL,
                stderr=sys.stdout if stderr_to_stdout else sys.stderr,
            )

            if result.returncode != 0:
                logger.error(f"Command exited with status {result.returncode}")

                if exit_on_error and result.returncode not in ignored_codes:
                    raise typer.Exit(result.returncode)

            logger.info(f"Command exited with status {result.returncode}")
        except KeyboardInterrupt:
            logger.info("Exiting...")
            raise typer.Exit(0)
        except Exception as e:
            logger.error(f"Error running command: {e}")
            continue


if __name__ == "__main__":
    typer.run(main)
