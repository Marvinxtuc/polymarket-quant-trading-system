from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class PersistedRuntimeState:
    runtime: dict[str, object] = field(default_factory=dict)
    control: dict[str, object] = field(default_factory=dict)
    risk: dict[str, object] = field(default_factory=dict)
    reconciliation: dict[str, object] = field(default_factory=dict)
    positions: list[dict[str, object]] = field(default_factory=list)
    order_intents: list[dict[str, object]] = field(default_factory=list)

    def to_payload(self) -> dict[str, object]:
        return {
            "runtime": dict(self.runtime),
            "control": dict(self.control),
            "risk": dict(self.risk),
            "reconciliation": dict(self.reconciliation),
            "positions": [dict(row) for row in self.positions],
            "order_intents": [dict(row) for row in self.order_intents],
        }
