from lead_aggregates.urls import canonical_hotel_url


def test_canonical_trailing_slash_normalized() -> None:
    a = canonical_hotel_url("https://WWW.Example.COM/foo/")
    b = canonical_hotel_url("https://www.example.com/foo")
    assert a == b
