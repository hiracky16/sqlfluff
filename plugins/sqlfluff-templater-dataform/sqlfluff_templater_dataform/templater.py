"""Defines the dbt templater.

NOTE: The dbt python package adds a significant overhead to import.
This module is also loaded on every run of SQLFluff regardless of
whether the dbt templater is selected in the configuration.

The templater is however only _instantiated_ when selected, and as
such, all imports of the dbt libraries are contained within the
DbtTemplater class and so are only imported when necessary.
"""

import logging
import os
import os.path
import re
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

from jinja2 import Environment
from jinja2_simple_tags import StandaloneTag

from sqlfluff.core.errors import SQLFluffSkipFile, SQLFluffUserError, SQLTemplaterError
from sqlfluff.core.templaters.base import RawTemplater, TemplatedFile, large_file_check
from sqlfluff.core.templaters.jinja import JinjaTemplater

# if TYPE_CHECKING:  # pragma: no cover

from sqlfluff.cli.formatters import OutputStreamFormatter
from sqlfluff.core import FluffConfig

# Instantiate the templater logger
templater_logger = logging.getLogger("sqlfluff.templater")


class DataformTemplater(RawTemplater):
    """A templater using dataform."""

    name = "dataform"
    sequential_fail_limit = 3
    adapters = {}

    def __init__(self, **kwargs):
        self.sqlfluff_config = None
        self.formatter = None
        self.project_id = None
        self.dataset_id = None
        self.working_dir = os.getcwd()
        self._sequential_fails = 0
        super().__init__(**kwargs)

    def sequence_files(
        self, fnames: List[str], config=None, formatter=None
    ) -> Iterator[str]:
        self.sqlfluff_config = config
        self.project_id = self.sqlfluff_config.get_section(
            (self.templater_selector, self.name, "project_id")
        )
        self.dataset_id = self.sqlfluff_config.get_section(
            (self.templater_selector, self.name, "dataset_id")
        )
        return fnames

    @large_file_check
    def process(
        self,
        *,
        fname: str,
        in_str: Optional[str] = None,
        config: Optional["FluffConfig"] = None,
        formatter: Optional["OutputStreamFormatter"] = None,
    ):
        if fname.endswith(".sqlx"):
            print(f"Processing SQLX file: {fname}")
        print(in_str)

        cleaned_content = self._replace_blocks_with_newline(in_str)
        cleaned_content = self.replace_ref_with_bq_table(cleaned_content)
        print(cleaned_content)

        return TemplatedFile(
            source_str=cleaned_content,
            fname=fname,
        ), []

    def _replace_blocks_with_newline(self, in_str: str) -> str:
        pattern = re.compile(r'(config|js)\s*\{([^}]*)\}', re.DOTALL)
        def replace(match):
            block_content = match.group(2)
            lines = block_content.count('\n')  # count new line
            print(lines)
            return '\n' * lines

        return re.sub(pattern, replace, in_str)

    def replace_ref_with_bq_table(self, sql):
        pattern = re.compile(r"\${ref\('([^']+)'(?:, '([^']+)')?\)}")
        def ref_to_table(match):
            if match.group(2):
                dataset = match.group(1)
                model_name = match.group(2)
            else:
                dataset = self.dataset_id
                model_name = match.group(1)
            return f"`{self.project_id}.{dataset}.{model_name}`"

        return re.sub(pattern, ref_to_table, sql)