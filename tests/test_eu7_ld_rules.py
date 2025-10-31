from __future__ import annotations

from src.app.rules.engine import evaluate_eu7_ld


def test_evaluate_eu7_ld_has_required_blocks() -> None:
    payload = evaluate_eu7_ld(None)

    assert payload["visual"]["map"] is not None
    assert payload["visual"]["chart"] is not None
    assert isinstance(payload.get("kpi_numbers"), list)
    assert isinstance(payload.get("sections"), list)
    assert isinstance(payload.get("final"), dict)
    assert isinstance(payload["final"].get("pass"), bool)

    assert payload.get("sections"), "Expected at least one section"
    has_boolean = False
    for section in payload["sections"]:
        criteria = section.get("criteria") if isinstance(section, dict) else []
        for row in criteria:
            if isinstance(row, dict) and "pass" in row:
                assert isinstance(row["pass"], bool)
                has_boolean = True
    assert has_boolean, "Section rows should include boolean pass flags"
