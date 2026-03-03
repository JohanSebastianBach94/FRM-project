from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

Frequency = Literal["daily", "monthly"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def default_run_id(prefix: str = "run") -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{stamp}"


@dataclass(frozen=True)
class ScenarioRunConfig:
    run_id: str
    created_at: str
    frequency: Frequency
    isos: List[str]
    t0: Optional[str]
    window_days: int
    allow_warn: bool
    allow_not_ready: bool
    filter_not_ready_blocks: bool
    freeze_step6_columns: bool


@dataclass(frozen=True)
class IsoFrozenInputs:
    iso: str
    factors: List[str]
    shortlist_factors: Optional[List[str]]
    shortlist_source: str
    dt_source: Optional[str]
    sigma_pairs_source: str
    rt_pairs_source: str
    eigenvalues_source: str
    adcc_meta_source: Optional[str]
    asof_t0: Optional[str]
    frozen_start: Optional[str]
    frozen_end: Optional[str]
    n_obs: Optional[int]

    # Optional: per-block readiness gate evidence (Step 8)
    readiness_allowed_blocks: Optional[List[str]]
    readiness_excluded_blocks: Optional[Dict[str, Any]]

    # Optional: hashes for frozen files we wrote into the run folder
    frozen_files: Optional[Dict[str, Any]]


@dataclass(frozen=True)
class ScenarioRunManifest:
    config: Dict[str, Any]
    pipeline_status: Optional[Dict[str, Any]]
    postfit_summary: Optional[Dict[str, Any]]
    iso_inputs: Dict[str, Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
