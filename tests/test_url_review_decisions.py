from pathlib import Path

from url_review.decisions import append_domain, load_domains, remove_domain


def test_append_domain_is_idempotent(tmp_path: Path) -> None:
    yes = tmp_path / "yes.txt"
    assert append_domain(yes, "Example.com")
    assert not append_domain(yes, "example.com")
    assert yes.read_text(encoding="utf-8").splitlines() == ["example.com"]


def test_load_domains_normalizes_and_filters(tmp_path: Path) -> None:
    yes = tmp_path / "yes.txt"
    yes.write_text("  https://www.HotelA.com  \nfoo.com\n\n", encoding="utf-8")
    assert load_domains(yes) == {"hotela.com", "foo.com"}


def test_remove_domain_is_idempotent_and_targeted(tmp_path: Path) -> None:
    yes = tmp_path / "yes.txt"
    assert append_domain(yes, "example.com")
    assert append_domain(yes, "another.com")
    assert remove_domain(yes, "https://www.example.com/path")
    assert load_domains(yes) == {"another.com"}
    assert remove_domain(yes, "example.com") is False

