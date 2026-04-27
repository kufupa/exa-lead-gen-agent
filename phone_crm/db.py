from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row

from phone_crm.config import Settings


@contextmanager
def open_connection(settings: Settings) -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(settings.database_url, row_factory=dict_row) as conn:
        yield conn
