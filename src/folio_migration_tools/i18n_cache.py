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
are handled correctly - parameters and the active locale are included in the cache key.
"""

from functools import lru_cache

import i18n


class TranslatedStr(str):
    """A translated string that remembers the key and parameters it came from.

    Behaves exactly like ``str``. The report store uses ``key`` and ``params`` to
    persist locale-neutral data so reports can be re-rendered in another language.
    The wrapper is lost as soon as the string is concatenated or formatted.
    """

    key: str
    params: dict

    def __new__(cls, text: str, key: str, params: dict):
        obj = super().__new__(cls, text)
        obj.key = key
        obj.params = params
        return obj

    @property
    def parts(self) -> tuple:
        """A TranslatedStr is a single part."""
        return (self,)

    def __getnewargs__(self):
        """Support copy and pickle, since __new__ needs the key and params."""
        return (str(self), self.key, self.params)

    def __add__(self, other):
        """Concatenate, returning a ComposedStr that keeps the translation key."""
        if not isinstance(other, str):
            return NotImplemented
        return _compose(self, other)

    def __radd__(self, other):
        """Concatenate on the left, returning a ComposedStr that keeps the key."""
        if not isinstance(other, str):
            return NotImplemented
        return _compose(other, self)


def _parts_of(value) -> tuple:
    """Return the translated/plain parts a string is made of."""
    if isinstance(value, (TranslatedStr, ComposedStr)):
        return value.parts
    return (str(value),)


def _compose(left, right) -> "ComposedStr":
    parts = []
    for part in _parts_of(left) + _parts_of(right):
        if parts and type(part) is str and type(parts[-1]) is str:
            parts[-1] += part
        else:
            parts.append(part)
    return ComposedStr(str(left) + str(right), tuple(parts))


class ComposedStr(str):
    """Concatenation of translated and plain text that remembers its parts.

    Created by adding to a TranslatedStr, e.g. ``i18n_t("Failed") + f": {reason}"``.
    Each part is either a TranslatedStr (key and params) or a plain str, so the
    report store can re-render the text in another locale.
    """

    parts: tuple

    def __new__(cls, text: str, parts: tuple):
        obj = super().__new__(cls, text)
        obj.parts = parts
        return obj

    def __getnewargs__(self):
        """Support copy and pickle, since __new__ needs the parts."""
        return (str(self), self.parts)

    def __add__(self, other):
        """Concatenate, keeping the parts."""
        if not isinstance(other, str):
            return NotImplemented
        return _compose(self, other)

    def __radd__(self, other):
        """Concatenate on the left, keeping the parts."""
        if not isinstance(other, str):
            return NotImplemented
        return _compose(other, self)


@lru_cache(maxsize=2048)
def _cached_t(key: str, locale: str) -> TranslatedStr:
    return TranslatedStr(i18n.t(key, locale=locale), key, {})


def i18n_t(key: str, **kwargs) -> str:
    """Wrapper around i18n.t() that caches static (parameterless) translations.

    This function caches the results of i18n.t() calls to avoid repeated
    translation lookups. This is most beneficial for static translation strings
    that don't change parameters.

    The active locale is part of the cache key, so changing the locale with
    i18n.set("locale", ...) takes effect immediately without clearing the cache.

    Parameterized translations are not cached, because their values can have a very
    large number of distinct permutations. They are still returned as TranslatedStr,
    so the key and params are preserved for the report store.

    Args:
        key: The translation key to look up
        **kwargs: Interpolation parameters passed to i18n.t()

    Returns:
        A TranslatedStr. Parameterless results are cached per key and locale.

    Note:
        The cache is module-level and persists for the lifetime of the process.
        If translation files or load paths change at runtime, call
        clear_i18n_cache() to invalidate the cache.
    """
    locale = kwargs.pop("locale", None) or i18n.get("locale")
    if kwargs:
        # Parameter values can have hundreds of thousands of distinct permutations,
        # which would evict the static entries, so parameterized calls bypass the cache.
        return TranslatedStr(i18n.t(key, locale=locale, **kwargs), key, dict(kwargs))
    return _cached_t(key, locale)


def clear_i18n_cache() -> None:
    """Clear the i18n translation cache.

    Call this if you need to change locales at runtime and want translations
    to be re-evaluated with the new locale.
    """
    _cached_t.cache_clear()


def get_i18n_cache_info() -> tuple:
    """Get cache statistics for monitoring and debugging.

    Returns:
        A named tuple with fields: hits, misses, maxsize, currsize
    """
    return _cached_t.cache_info()
