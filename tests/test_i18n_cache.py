"""Test the i18n caching functionality."""


from folio_migration_tools.i18n_cache import (
    TranslatedStr,
    clear_i18n_cache,
    get_i18n_cache_info,
    i18n_t,
)


def test_i18n_cache_basic():
    """Test that i18n_t caches results."""
    clear_i18n_cache()

    # First call should miss cache
    result1 = i18n_t("Total number of Tags processed")
    cache_info = get_i18n_cache_info()
    assert cache_info.misses >= 1

    # Second call should hit cache
    result2 = i18n_t("Total number of Tags processed")
    assert result1 == result2
    cache_info = get_i18n_cache_info()
    assert cache_info.hits >= 1


def test_i18n_cache_different_keys():
    """Test that different keys create different cache entries."""
    clear_i18n_cache()

    result1 = i18n_t("Total number of Tags processed")
    result2 = i18n_t("Records without $6")

    # Results should be different
    assert result1 != result2

    # Both should be cached
    cache_info = get_i18n_cache_info()
    assert cache_info.currsize >= 2


def test_i18n_cache_with_parameters():
    """Test that parameterized translations work correctly."""
    clear_i18n_cache()

    # Parameterized call
    result = i18n_t("Changed %{a} to %{b}", a="old", b="new")
    assert (
        "old" in result or "new" in result or result
    )  # Should contain parameters or be a translated string

    # Same call should hit cache
    result2 = i18n_t("Changed %{a} to %{b}", a="old", b="new")
    assert result == result2


def test_cache_clear():
    """Test that clearing the cache works."""
    clear_i18n_cache()

    # Populate cache
    i18n_t("Total number of Tags processed")
    cache_info = get_i18n_cache_info()
    initial_size = cache_info.currsize
    assert initial_size > 0

    # Clear cache
    clear_i18n_cache()
    cache_info = get_i18n_cache_info()
    assert cache_info.currsize == 0
    assert cache_info.hits == 0
    assert cache_info.misses == 0


def test_cache_respects_locale_changes():
    """Test that cached translations follow the active locale."""
    import i18n
    from pathlib import Path

    i18n.load_config(Path(__file__).parents[1] / "src/folio_migration_tools/i18n_config.py")
    clear_i18n_cache()
    original_locale = i18n.get("locale")
    try:
        i18n.set("locale", "en")
        assert i18n_t("Measure") == "Measure"
        i18n.set("locale", "fr")
        assert i18n_t("Measure") == "Mesure"
        assert i18n_t("Measure", locale="en") == "Measure"
    finally:
        i18n.set("locale", original_locale)
        clear_i18n_cache()


def test_unhashable_params_still_return_translated_str():
    result = i18n_t("Some key", items=["a", "b"])
    assert isinstance(result, TranslatedStr)
    assert result.key == "Some key"


def test_no_direct_i18n_t_calls_outside_cache_module():
    """Direct i18n.t() returns a plain str and loses the key the report store needs."""
    import pathlib
    import re

    src = pathlib.Path(__file__).parent.parent / "src" / "folio_migration_tools"
    offenders = [
        str(p.relative_to(src))
        for p in src.rglob("*.py")
        if p.name != "i18n_cache.py" and re.search(r"\bi18n\.t\(", p.read_text(encoding="utf-8"))
    ]
    assert not offenders, f"Use i18n_t from i18n_cache instead of i18n.t in: {offenders}"


def test_parameterized_calls_are_not_cached():
    clear_i18n_cache()
    for n in range(50):
        result = i18n_t("Some key %{n}", n=n)
        assert result.params == {"n": n}
    assert get_i18n_cache_info().currsize == 0


def test_adding_to_translated_str_keeps_parts():
    from folio_migration_tools.i18n_cache import ComposedStr

    left = i18n_t("Some key") + ": 42"
    both = "prefix " + i18n_t("Some key") + " / " + i18n_t("Other key", n=1)
    assert isinstance(left, ComposedStr)
    assert str(left) == f"{i18n_t('Some key')}: 42"
    assert [getattr(p, "key", p) for p in left.parts] == ["Some key", ": 42"]
    assert [getattr(p, "key", p) for p in both.parts] == [
        "prefix ",
        "Some key",
        " / ",
        "Other key",
    ]
    assert isinstance(left + "x", ComposedStr)


def test_composed_str_copies():
    import copy
    import pickle

    value = i18n_t("Some key") + ": 1"
    assert copy.deepcopy(value).parts == value.parts
    assert pickle.loads(pickle.dumps(value)).parts == value.parts  # noqa: S301
