from src.app.data.regulation import evaluate_pack, load_pack


def test_load_regulation_pack_from_json() -> None:
    pack = load_pack("data/regpacks/eu7_demo.json")
    assert pack.id == "eu7_demo"
    assert pack.title == "EU7 (Demo)"
    assert len(pack.rules) == 4
    assert pack.rules[0].mandatory is True


def test_evaluate_pack_success() -> None:
    pack = load_pack("data/regpacks/eu7_demo.json")
    analysis_payload = {
        "overall": {"valid": True},
        "bins": {
            "urban": {
                "distance_km": 6.0,
                "time_s": 620.0,
                "kpis": {"NOx_mg_per_km": 250.0, "PN_1_per_km": 5e11},
            },
            "rural": {
                "distance_km": 5.6,
                "time_s": 480.0,
                "kpis": {"NOx_mg_per_km": 180.0},
            },
        },
    }

    evaluation = evaluate_pack(analysis_payload, pack)
    assert evaluation.overall_passed is True
    assert evaluation.mandatory_passed == evaluation.mandatory_total
    assert evaluation.optional_passed == evaluation.optional_total == 1

    cov_rule = next(item for item in evaluation.evidence if item.rule.id == "cov_urban_min")
    assert cov_rule.passed is True
    assert cov_rule.actual == 6.0
    assert cov_rule.margin == 1.0

    kpi_rule = next(item for item in evaluation.evidence if item.rule.id == "kpi_nox_urban_max")
    assert kpi_rule.passed is True
    assert kpi_rule.actual == 250.0


def test_evaluate_pack_missing_metric_marks_failure() -> None:
    pack = load_pack("data/regpacks/eu7_demo.json")
    analysis_payload = {
        "bins": {
            "urban": {
                "distance_km": 4.0,
                "time_s": 200.0,
                "kpis": {"NOx_mg_per_km": 450.0},
            }
        }
    }

    evaluation = evaluate_pack(analysis_payload, pack)
    assert evaluation.overall_passed is False
    assert evaluation.mandatory_passed < evaluation.mandatory_total

    rural_rule = next(item for item in evaluation.evidence if item.rule.id == "cov_rural_min")
    assert rural_rule.passed is False
    assert rural_rule.detail is not None
    assert "not" in rural_rule.detail.lower()

    kpi_rule = next(item for item in evaluation.evidence if item.rule.id == "kpi_nox_urban_max")
    assert kpi_rule.passed is False
    assert kpi_rule.actual == 450.0
    assert kpi_rule.margin == -150.0
