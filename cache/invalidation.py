from __future__ import annotations

from collections.abc import Iterable

from cache.instance import cache


TITLE_COLLECTION_NAMESPACES = (
    'titles:list',
    'titles:top',
    'titles:search',
    'titles:discover',
)

TITLE_DETAIL_NAMESPACES = (
    'title:detail',
    'title:full',
)

ANALYTICS_NAMESPACES = (
    'analytics:top-genres',
)


def invalidate_namespaces(namespaces: Iterable[str]) -> None:
    cache.bump_namespace_versions(tuple(namespaces))


def invalidate_title_collections() -> None:
    invalidate_namespaces((*TITLE_COLLECTION_NAMESPACES, *ANALYTICS_NAMESPACES))


def invalidate_title(title_id: int | None = None) -> None:
    namespaces = [*TITLE_COLLECTION_NAMESPACES, *ANALYTICS_NAMESPACES]

    namespaces.extend(TITLE_DETAIL_NAMESPACES)
    invalidate_namespaces(namespaces)


def invalidate_person(person_id: int | None = None) -> None:
    namespaces = []

    namespaces.append('person:full')
    namespaces.extend(TITLE_COLLECTION_NAMESPACES)
    invalidate_namespaces(namespaces)


def invalidate_all_application_cache() -> None:
    invalidate_namespaces((
        *TITLE_COLLECTION_NAMESPACES,
        *TITLE_DETAIL_NAMESPACES,
        'person:full',
        *ANALYTICS_NAMESPACES,
    ))
