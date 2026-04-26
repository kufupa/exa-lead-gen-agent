from __future__ import annotations

from pipeline.config import PipelineConfig
from pipeline.models import CandidateLead, HotelOrg, SourceRef
from pipeline.source_pack import build_source_pack, source_pack_to_json


def test_source_pack_bounds_and_json() -> None:
    hotel = HotelOrg(input_url="https://hotel.example", domains=["hotel.example"])
    long_snip = "x" * 10_000
    ev = SourceRef(url="https://u1", snippet=long_snip, query="q")
    c = CandidateLead(
        candidate_id="c1",
        full_name="A B",
        title="GM",
        role_tier=1,
        role_family="gm_ops",
        current_role_confidence="high",
        evidence=[ev],
    )
    cfg = PipelineConfig(max_source_chars_per_ref=4000)
    pack = build_source_pack(hotel, [c], [], cfg)
    js = source_pack_to_json(pack)
    assert "hotel.example" in js
    sn = pack["candidate_groups"][0]["sources"][0]["snippet"]
    assert len(sn) < len(long_snip)
