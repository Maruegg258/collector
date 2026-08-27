from __future__ import annotations

from typing import Any

from .storage_protocol import StorageBackend


ALLOWED_HANDOFF_GAP_REASONS = {"recentTrades_insufficient_overlap"}
HANDOFF_HEALED_REASON = "covered_by_stateless_handoff_overlap"


def parse_gap_spec(spec: str) -> list[tuple[int, int]]:
    pairs: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    for raw in (part.strip() for part in spec.split(",")):
        if not raw:
            continue
        pieces = raw.split(":", 1)
        if len(pieces) != 2:
            raise ValueError(f"invalid gap spec item: {raw!r}")
        start_ms = int(pieces[0])
        end_ms = int(pieces[1])
        if start_ms <= 0 or end_ms <= start_ms:
            raise ValueError(f"invalid gap interval: {raw!r}")
        pair = (start_ms, end_ms)
        if pair not in seen:
            pairs.append(pair)
            seen.add(pair)
    return pairs


def heal_configured_handoff_gaps(
    store: StorageBackend,
    *,
    coin: str,
    spec: str,
) -> dict[str, Any]:
    requested = parse_gap_spec(spec)
    healed: list[dict[str, Any]] = []
    missing: list[dict[str, int]] = []
    rejected: list[dict[str, Any]] = []

    for start_ms, end_ms in requested:
        matches = [
            gap
            for gap in store.gaps_overlapping(
                coin,
                start_ms,
                end_ms,
                status="UNRESOLVED",
            )
            if int(gap["start_ms"]) == start_ms
            and int(gap["end_ms"]) == end_ms
        ]
        if not matches:
            missing.append({"start_ms": start_ms, "end_ms": end_ms})
            continue
        if len(matches) != 1:
            rejected.append(
                {
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "reason": "non_unique_exact_match",
                }
            )
            continue

        gap = matches[0]
        original_reason = str(gap["reason"])
        if original_reason not in ALLOWED_HANDOFF_GAP_REASONS:
            rejected.append(
                {
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "reason": "unexpected_original_reason",
                    "original_reason": original_reason,
                }
            )
            continue

        store.add_gap(
            coin,
            start_ms,
            end_ms,
            status="HEALED",
            reason=HANDOFF_HEALED_REASON,
            recovery_earliest_ms=gap.get("recovery_earliest_ms"),
            recovery_latest_ms=gap.get("recovery_latest_ms"),
            recovered_trade_count=int(gap.get("recovered_trade_count") or 0),
        )
        healed.append(
            {
                "start_ms": start_ms,
                "end_ms": end_ms,
                "original_reason": original_reason,
            }
        )

    return {
        "requested": len(requested),
        "healed": len(healed),
        "missing": len(missing),
        "rejected": len(rejected),
        "healed_intervals": healed,
        "missing_intervals": missing,
        "rejected_intervals": rejected,
        "verified": len(healed) == len(requested)
        and not missing
        and not rejected,
    }
