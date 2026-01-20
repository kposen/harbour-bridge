from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from src.domain.schemas import FinancialModel


DATA_DIR = Path(__file__).resolve().parents[2] / "data"


def save_share_data(ticker: str, data: FinancialModel) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = _share_path(ticker)
    payload = data.model_dump(mode="json")
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def load_share_data(ticker: str) -> Optional[FinancialModel]:
    path = _share_path(ticker)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return FinancialModel.model_validate(payload)


def _share_path(ticker: str) -> Path:
    normalized = ticker.strip().upper()
    return DATA_DIR / f"{normalized}.json"
