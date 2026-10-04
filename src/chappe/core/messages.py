# src/chappe/core/messages.py
"""What a theme returns."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ParentMessage:
    text: str


@dataclass(frozen=True, slots=True)
class ThreadEntry:
    key: str
    text: str
    broadcast: bool = False


@dataclass(frozen=True, slots=True)
class Alert:
    key: str
    text: str


@dataclass(frozen=True, slots=True)
class MessageSet:
    parent: ParentMessage
    thread: tuple[ThreadEntry, ...] = ()
    alerts: tuple[Alert, ...] = ()
