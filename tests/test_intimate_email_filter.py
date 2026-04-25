from lead_aggregates.builders import has_named_email


def test_named_email_included() -> None:
    assert has_named_email({"email": "jane.doe@example.com"})


def test_generic_reservations_excluded() -> None:
    assert not has_named_email({"email": "reservations@example.com"})


def test_email2_named_counts() -> None:
    assert has_named_email({"email": "reservations@example.com", "email2": "person.name@example.com"})
