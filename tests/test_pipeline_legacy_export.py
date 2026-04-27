from __future__ import annotations

import json
from pathlib import Path

import pipeline.cli as cli
import pytest
from pipeline.cli import run_pipeline
from pipeline.config import PipelineConfig
from pipeline.legacy_export import (
    load_pipeline_artifact,
    load_pipeline_ui_json,
    persist_pipeline_ui,
    pipeline_ui_to_enriched_doc,
)
from pipeline.models import (
    CandidateLead,
    ContactRoute,
    HotelOrg,
    PipelineTelemetry,
    PipelineUiJson,
    SourceRef,
)
from scripts.import_pipeline_outputs import import_pipeline_outputs, select_latest_pipeline_results


def _sample_ui() -> PipelineUiJson:
    hotel = HotelOrg(
        input_url="examplehotel.com",
        canonical_name="Example Hotel",
        property_name="Example Hotel",
        domains=["examplehotel.com"],
    )
    candidate = CandidateLead(
        candidate_id="c_1",
        full_name="Alex Person",
        title="General Manager",
        company="Example Hotel",
        role_tier=1,
        role_family="gm_ops",
        current_role_confidence="high",
        relationship_confidence="high",
        linkedin_url="https://uk.linkedin.com/in/alex-person",
        contact_routes=[
            ContactRoute(
                kind="email",
                value="alex@examplehotel.com",
                confidence="high",
                source_url="https://examplehotel.com/team",
            ),
            ContactRoute(
                kind="phone",
                value="+44 20 0000 0000",
                confidence="medium",
                source_url="https://examplehotel.com/contact",
            ),
            ContactRoute(
                kind="generic_email",
                value="sales@examplehotel.com",
                confidence="medium",
                source_url="https://examplehotel.com/contact",
            ),
        ],
        evidence=[
            SourceRef(
                url="https://examplehotel.com/team",
                title="Team",
                source_type="hotel_site",
                snippet="Alex Person is General Manager.",
            )
        ],
        reason_kept="GM owns hotel operations.",
        notes=["Verified from official site."],
    )
    return PipelineUiJson(
        input_url="examplehotel.com",
        resolved_org=hotel,
        aliases=[],
        candidates=[candidate],
        provider_costs={"total_usd": 0.12},
        quality_metrics={"candidate_count": 1},
        telemetry=PipelineTelemetry(),
    )


def test_pipeline_ui_to_enriched_doc_maps_direct_routes_and_preserves_v4_payload() -> None:
    doc = pipeline_ui_to_enriched_doc(_sample_ui(), generated_at_utc="2026-04-26T22:00:00+00:00")

    assert doc["target_url"] == "examplehotel.com"
    assert doc["generated_at_utc"] == "2026-04-26T22:00:00+00:00"
    assert doc["model"] == "pipeline-v4"
    assert doc["pipeline_v4"]["input_url"] == "examplehotel.com"

    contact = doc["contacts"][0]
    assert contact["full_name"] == "Alex Person"
    assert contact["pipeline_v4_candidate_id"] == "c_1"
    assert contact["email"] == "alex@examplehotel.com"
    assert contact["email2"] is None
    assert contact["phone"] == "+44 20 0000 0000"
    assert contact["phone2"] is None
    assert contact["decision_maker_score"] == "high"
    assert contact["intimacy_grade"] == "high"
    assert "sales@examplehotel.com" in (contact["other_contact_detail"] or "")
    assert contact["evidence"][0]["source_url"] == "https://examplehotel.com/team"
    assert contact["evidence"][0]["source_type"] == "official_site"


def test_persist_pipeline_ui_writes_jsons_and_rebuilds_fulljsons(tmp_path: Path) -> None:
    jsons_dir = tmp_path / "jsons"
    fulljsons_dir = tmp_path / "fullJSONs"

    enriched_path = persist_pipeline_ui(_sample_ui(), jsons_dir=jsons_dir, fulljsons_dir=fulljsons_dir)

    assert enriched_path.name.endswith(".enriched.json")
    assert enriched_path.exists()
    assert (fulljsons_dir / "all_enriched_leads.json").exists()
    assert (fulljsons_dir / "intimate_email_contacts.json").exists()
    assert (fulljsons_dir / "intimate_phone_contacts.json").exists()
    assert (fulljsons_dir / "intimate_unified_contacts.json").exists()
    assert (fulljsons_dir / "url_registry.json").exists()

    phone = json.loads((fulljsons_dir / "intimate_phone_contacts.json").read_text(encoding="utf-8"))
    email = json.loads((fulljsons_dir / "intimate_email_contacts.json").read_text(encoding="utf-8"))
    unified = json.loads((fulljsons_dir / "intimate_unified_contacts.json").read_text(encoding="utf-8"))
    assert phone["count"] == 1
    assert email["count"] == 1
    assert unified["count"] == 1


def test_select_latest_pipeline_results_keeps_latest_per_canonical_url(tmp_path: Path) -> None:
    old_dir = tmp_path / "outputs" / "pipeline" / "20260426T100000Z__examplehotel_com__old"
    new_dir = tmp_path / "outputs" / "pipeline" / "20260426T110000Z__examplehotel_com__new"
    other_dir = tmp_path / "outputs" / "pipeline" / "20260426T120000Z__otherhotel_com__new"
    old_dir.mkdir(parents=True)
    new_dir.mkdir(parents=True)
    other_dir.mkdir(parents=True)

    old_ui = _sample_ui()
    new_ui = _sample_ui().model_copy(update={"quality_metrics": {"candidate_count": 2}})
    other_ui = _sample_ui().model_copy(update={"input_url": "https://otherhotel.com"})

    (old_dir / "pipeline_result.json").write_text(old_ui.model_dump_json(indent=2), encoding="utf-8")
    (new_dir / "pipeline_result.json").write_text(new_ui.model_dump_json(indent=2), encoding="utf-8")
    (other_dir / "pipeline_result.json").write_text(other_ui.model_dump_json(indent=2), encoding="utf-8")

    selected = select_latest_pipeline_results(tmp_path / "outputs" / "pipeline")

    assert selected == [new_dir / "pipeline_result.json", other_dir / "pipeline_result.json"]


def test_run_pipeline_persists_ui_when_sync_enabled(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    sample = _sample_ui()
    calls: list[tuple[Path, Path]] = []

    monkeypatch.setattr(cli, "load_repo_dotenv", lambda _root: None)
    d = type(
        "D",
        (),
        {"hotel": sample.resolved_org, "aliases": [], "drafts": []},
    )()
    monkeypatch.setattr(
        cli,
        "run_grok_discovery",
        lambda _url, _key, _tel: (d, []),
    )
    monkeypatch.setattr(cli, "plan_exa_jobs", lambda *_args, **_kwargs: ([], False))
    monkeypatch.setattr(cli, "_load_exa_client", lambda: None)
    monkeypatch.setattr(
        cli,
        "promote_discovery_to_candidates",
        lambda *_args, **_kwargs: (sample.candidates, []),
    )
    monkeypatch.setattr(cli, "mine_contacts_v4", lambda _hotel, candidates, *_args: candidates)
    monkeypatch.setenv("XAI_API_KEY", "x")

    def _persist(
        _ui: PipelineUiJson,
        *,
        jsons_dir: Path,
        fulljsons_dir: Path,
        rebuild_fulljsons: bool = True,
    ) -> Path:
        calls.append((jsons_dir, fulljsons_dir))
        return jsons_dir / "x.enriched.json"

    monkeypatch.setattr(cli, "persist_pipeline_ui", _persist)

    run_pipeline(
        "examplehotel.com",
        PipelineConfig(),
        out_dir=tmp_path / "outputs" / "pipeline",
        jsons_dir=tmp_path / "jsons",
        fulljsons_dir=tmp_path / "fullJSONs",
        aggregate_sync=True,
    )

    assert calls == [(tmp_path / "jsons", tmp_path / "fullJSONs")]


def test_import_pipeline_outputs_round_trip(tmp_path: Path) -> None:
    out = tmp_path / "outputs" / "pipeline" / "20260426T110000Z__examplehotel_com__h"
    out.mkdir(parents=True)
    (out / "pipeline_result.json").write_text(_sample_ui().model_dump_json(indent=2), encoding="utf-8")

    jsons_dir = tmp_path / "jsons"
    fulljsons_dir = tmp_path / "fullJSONs"
    written = import_pipeline_outputs(
        tmp_path / "outputs" / "pipeline",
        jsons_dir,
        fulljsons_dir,
    )
    assert len(written) == 1
    data = json.loads(written[0].read_text(encoding="utf-8"))
    assert data["model"] == "pipeline-v4"
    assert (fulljsons_dir / "all_enriched_leads.json").exists()


def test_load_pipeline_ui_json_roundtrip(tmp_path: Path) -> None:
    f = tmp_path / "p.json"
    u = _sample_ui()
    f.write_text(u.model_dump_json(indent=2), encoding="utf-8")
    u2 = load_pipeline_ui_json(f)
    assert u2.input_url == u.input_url


def test_load_pipeline_artifact_accepts_run_result_dict(tmp_path: Path) -> None:
    u = _sample_ui()
    out = u.model_copy(update={"candidates": []})
    from pipeline.models import PipelineRunResult

    pr = PipelineRunResult(
        hotel=out.resolved_org,
        candidates=[],
        review_rows=[],
        telemetry=out.telemetry,
        source_pack_json=None,
    )
    f = tmp_path / "pr.json"
    f.write_text(pr.model_dump_json(indent=2), encoding="utf-8")
    ui = load_pipeline_artifact(f)
    assert ui.resolved_org.input_url == pr.hotel.input_url
    assert ui.input_url == pr.hotel.input_url
