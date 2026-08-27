from scripts.correct_reconnect_gap_20260827 import (
    TRUE_END_MS,
    TRUE_START_MS,
    classify_corrected_gap,
)


def test_correction_marks_healed_only_with_full_recovery_overlap():
    status, reason, proven = classify_corrected_gap(TRUE_START_MS - 1, TRUE_END_MS)
    assert status == "HEALED"
    assert reason == "bookkeeping_correction_recentTrades_overlap_proof"
    assert proven is True


def test_correction_keeps_short_gap_unresolved_without_overlap_proof():
    status, reason, proven = classify_corrected_gap(TRUE_START_MS + 1, TRUE_END_MS)
    assert status == "UNRESOLVED"
    assert reason == "bookkeeping_correction_unverified_short_reconnect"
    assert proven is False

    status, _, proven = classify_corrected_gap(None, None)
    assert status == "UNRESOLVED"
    assert proven is False
