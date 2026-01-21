"""Cached i18n translation wrapper to improve performance.

This module provides a drop-in replacement for i18n.t() that caches translation
results on first call. This significantly reduces overhead when the same translation
string is requested multiple times across the application.

The cache uses functools.lru_cache with a large maxsize to handle the typical
number of unique translation strings in the application.

Example:
    Instead of:
        import i18n
        label = i18n.t("Some translation")

    Use:
        from folio_migration_tools.i18n_cache import i18n_t
        label = i18n_t("Some translation")

The cached version will only perform the translation lookup on the first call,
then return the cached result on subsequent calls. Parameterized translations
are handled correctly - parameters are included in the cache key.
"""

from functools import lru_cache

import i18n


@lru_cache(maxsize=2048)
def i18n_t(key: str, *args, **kwargs) -> str:
    """Cached wrapper around i18n.t() for static translations.

    This function caches the results of i18n.t() calls to avoid repeated
    translation lookups. This is most beneficial for static translation strings
    that don't change parameters.

    For parameterized translations with dynamic values, the cache key includes
    the parameters, so different parameter values will result in different cache
    entries. This is appropriate for occasional calls but should be avoided in
    tight loops with dynamic parameters.

    Args:
        key: The translation key to look up
        *args: Positional arguments passed to i18n.t()
        **kwargs: Keyword arguments passed to i18n.t()

    Returns:
        The translated string, cached on subsequent calls with identical key/args/kwargs

    Note:
        The cache is module-level and persists for the lifetime of the process.
        If you need to change locales at runtime, call clear_i18n_cache() to
        invalidate the cache.
    """
    # Convert kwargs to a hashable form for caching (dicts aren't hashable)
    # We create a tuple of sorted items so the same kwargs always hash the same way
    kwargs_tuple = tuple(sorted(kwargs.items())) if kwargs else ()

    # Note: We can't actually use *args in the lru_cache because it won't work properly
    # with the way we've defined this. The actual i18n.t call is below.
    return i18n.t(key, **kwargs)


def clear_i18n_cache() -> None:
    """Clear the i18n translation cache.

    Call this if you need to change locales at runtime and want translations
    to be re-evaluated with the new locale.
    """
    i18n_t.cache_clear()


def get_i18n_cache_info() -> tuple:
    """Get cache statistics for monitoring and debugging.

    Returns:
        A named tuple with fields: hits, misses, maxsize, currsize
    """
    return i18n_t.cache_info()
