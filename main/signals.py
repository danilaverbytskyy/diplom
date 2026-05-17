from django.db import transaction
from django.db.models.signals import m2m_changed, post_delete, post_save
from django.dispatch import receiver

from cache.invalidation import (
    invalidate_person,
    invalidate_title,
    invalidate_title_collections,
)
from main.models import Crew, Genre, Person, Principal, Rating, Title


def on_commit(callback):
    transaction.on_commit(callback)


@receiver(post_save, sender=Title)
@receiver(post_delete, sender=Title)
def invalidate_title_cache(sender, instance: Title, **kwargs):
    on_commit(lambda: invalidate_title(instance.id))


@receiver(post_save, sender=Rating)
@receiver(post_delete, sender=Rating)
def invalidate_rating_cache(sender, instance: Rating, **kwargs):
    on_commit(lambda: invalidate_title(instance.title_id))


@receiver(post_save, sender=Genre)
@receiver(post_delete, sender=Genre)
def invalidate_genre_cache(sender, instance: Genre, **kwargs):
    on_commit(invalidate_title_collections)


@receiver(m2m_changed, sender=Title.genres.through)
def invalidate_title_genres_cache(sender, instance: Title, action: str, **kwargs):
    if action in {'post_add', 'post_remove', 'post_clear'}:
        on_commit(lambda: invalidate_title(instance.id))


@receiver(post_save, sender=Person)
@receiver(post_delete, sender=Person)
def invalidate_person_cache(sender, instance: Person, **kwargs):
    on_commit(lambda: invalidate_person(instance.id))


@receiver(post_save, sender=Principal)
@receiver(post_delete, sender=Principal)
def invalidate_principal_cache(sender, instance: Principal, **kwargs):
    title_id = instance.title_id
    person_id = instance.person_id
    on_commit(lambda: (invalidate_title(title_id), invalidate_person(person_id)))


@receiver(post_save, sender=Crew)
@receiver(post_delete, sender=Crew)
def invalidate_crew_cache(sender, instance: Crew, **kwargs):
    title_id = instance.title_id
    person_id = instance.person_id
    on_commit(lambda: (invalidate_title(title_id), invalidate_person(person_id)))
