from __future__ import annotations

import json
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel


class JsonlRecorder:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def record(self, event_type: str, payload: BaseModel | dict[str, Any]) -> None:
        if isinstance(payload, BaseModel):
            data = payload.model_dump(mode="json")
        else:
            data = payload
        envelope = {
            "recorded_ts": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type,
            "payload": data,
        }
        with suppress(KeyboardInterrupt):
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(envelope, separators=(",", ":"), sort_keys=True) + "\n")


class ReplayReader:
    def __init__(self, path: Path):
        self.path = path

    def iter_events(self) -> Any:
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                yield json.loads(line)
