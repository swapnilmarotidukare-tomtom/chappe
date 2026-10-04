# src/chappe/ports/store.py
from __future__ import annotations

from typing import Protocol

from chappe.core.reconcile import SentState


class Store(Protocol):
    def load(self, process_key: str) -> SentState | None:
        """The stored state, or None when nothing is stored. Raises StoreError when unreadable."""
        ...

    def save(self, process_key: str, state: SentState) -> SentState:
        """Re-read the stored state, write `merge_sent(stored, state)` and return the merge.

        Never overwrites: two writers that save at the same time both keep their keys.
        """
        ...
