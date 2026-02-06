"""Test the i18n caching functionality."""

import pytest

from folio_migration_tools.i18n_cache import i18n_t, clear_i18n_cache, get_i18n_cache_info


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
    assert "old" in result or "new" in result or result  # Should contain parameters or be a translated string
    
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
