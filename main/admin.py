from django.contrib import admin
from .models import Genre, Title, TitleRating, Person, TitlePrincipal, TitleCrew


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ("id", "name")
    search_fields = ("name",)
    ordering = ("name",)


class TitleRatingInline(admin.StackedInline):
    model = TitleRating
    extra = 0
    can_delete = False


class TitleCrewInline(admin.TabularInline):
    model = TitleCrew
    extra = 0
    autocomplete_fields = ("person",)
    verbose_name = "Участник съемочной группы"
    verbose_name_plural = "Участники съемочной группы"


class TitlePrincipalInline(admin.TabularInline):
    model = TitlePrincipal
    extra = 0
    autocomplete_fields = ("person",)
    verbose_name = "Основной участник"
    verbose_name_plural = "Основные участники"


@admin.register(Title)
class TitleAdmin(admin.ModelAdmin):
    list_display = (
        "tconst",
        "primary_title",
        "title_type",
        "start_year",
        "is_adult",
        "display_rating",
        "display_votes",
    )
    list_filter = ("title_type", "is_adult", "start_year", "genres")
    search_fields = ("primary_title",)
    autocomplete_fields = ()
    filter_horizontal = ("genres",)
    inlines = (TitleRatingInline, TitleCrewInline, TitlePrincipalInline)

    fieldsets = (
        ("Основная информация", {
            "fields": (
                "tconst",
                "title_type",
                "primary_title",
                "original_title",
            )
        }),
        ("Дополнительные данные", {
            "fields": (
                "is_adult",
                "start_year",
                "end_year",
                "runtime_minutes",
                "genres",
            )
        }),
    )

    @admin.display(description="Рейтинг")
    def display_rating(self, obj):
        return obj.rating.average_rating if hasattr(obj, "rating") else None

    @admin.display(description="Голосов")
    def display_votes(self, obj):
        return obj.rating.num_votes if hasattr(obj, "rating") else None


@admin.register(TitleRating)
class TitleRatingAdmin(admin.ModelAdmin):
    list_display = ("title", "average_rating", "num_votes")
    search_fields = ("title__primary_title", "title__tconst")
    ordering = ("-average_rating", "-num_votes")
    autocomplete_fields = ("title",)


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = (
        "nconst",
        "primary_name",
        "birth_year",
        "death_year",
        "display_professions",
    )
    search_fields = ("nconst", "primary_name")
    ordering = ("primary_name",)
    filter_horizontal = ("known_for_titles",)

    @admin.display(description="Профессии")
    def display_professions(self, obj):
        return ", ".join(obj.primary_professions) if obj.primary_professions else "-"


@admin.register(TitlePrincipal)
class TitlePrincipalAdmin(admin.ModelAdmin):
    list_display = (
        "title",
        "person",
        "ordering",
        "category",
        "job",
        "display_characters",
    )
    list_filter = ("category",)
    search_fields = (
        "title__primary_title",
        "title__tconst",
        "person__primary_name",
        "person__nconst",
        "job",
    )
    ordering = ("title", "ordering")
    autocomplete_fields = ("title", "person")

    @admin.display(description="Персонажи")
    def display_characters(self, obj):
        if not obj.characters:
            return "-"
        if isinstance(obj.characters, list):
            return ", ".join(obj.characters)
        return str(obj.characters)


@admin.register(TitleCrew)
class TitleCrewAdmin(admin.ModelAdmin):
    list_display = ("title", "person", "role")
    list_filter = ("role",)
    search_fields = (
        "title__primary_title",
        "title__tconst",
        "person__primary_name",
        "person__nconst",
    )
    ordering = ("title", "role", "person")
    autocomplete_fields = ("title", "person")