from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import colorama

if TYPE_CHECKING:
    from pydantic import ValidationInfo

    from ._types import DBType
    from .query import Query

__all__ = (
    "MISSING",
    "validate",
    "log",
    "log_sql",
    "log_res",
    "log_select",
    "log_insert",
    "log_update",
    "log_delete",
)

log = logging.getLogger(__name__)


def log_sql(q: Query, _type: str, color):
    """実行するsqlのログ"""
    log.debug(
        f"{color}======DEBUG {_type}================================================={colorama.Fore.RESET}"
    )
    log.debug(q.to_string())
    log.debug(
        f"{color}==================================================================={colorama.Fore.RESET}"
    )


def log_delsql(q: Query, _type: str, color):
    """実行するsqlのログ"""
    log.warn(
        f"{color}======DEBUG {_type}================================================={colorama.Fore.RESET}"
    )
    log.warn(q.to_string())
    log.warn(
        f"{color}==================================================================={colorama.Fore.RESET}"
    )


def log_select(q: Query):
    """ログ"""
    log_sql(q, "SELECT", colorama.Fore.GREEN)


def log_insert(q: Query):
    """ログ"""
    log_sql(q, "INSERT", colorama.Fore.LIGHTMAGENTA_EX)


def log_update(q: Query):
    """ログ"""
    log_sql(q, "UPDATE", colorama.Fore.CYAN)


def log_delete(q: Query):
    """ログ"""
    log_delsql(q, "DELETE", colorama.Fore.LIGHTRED_EX)


def log_res(res):
    """レスポンスログ"""
    log.debug(f"RESPONSE: {res}")


def validate(v: DBType, info: ValidationInfo):
    """pydantic BaseModelでstrやdictに変換できない型を定義する用"""
    return v


class _MissingSentinel:
    __slots__ = ()

    def __eq__(self, other) -> bool:
        return False

    def __bool__(self) -> bool:
        return False

    def __hash__(self) -> int:
        return 0

    def __repr__(self):
        return "..."


MISSING: Any = _MissingSentinel()
