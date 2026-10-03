import chappe


def test_version_is_the_development_version() -> None:
    assert chappe.__version__ == "0.0.1.dev0"
