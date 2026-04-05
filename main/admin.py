from django.contrib import admin

from main.models import Crew, Genre, Person, Principal, Rating, Title


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('id', 'name')
    search_fields = ('name',)


@admin.register(Title)
class TitleAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'tconst',
        'title',
        'display_title_type',
        'is_adult',
        'start_year',
        'end_year',
        'runtime_minutes',
    )
    search_fields = ('tconst', 'title')

    @admin.display(description='Тип произведения')
    def display_title_type(self, obj):
        return obj.get_title_type_display()


@admin.register(Rating)
class RatingAdmin(admin.ModelAdmin):
    list_display = ('title', 'display_average_rating', 'num_votes')
    search_fields = ('title__tconst', 'title__title')

    @admin.display(description='Средний рейтинг')
    def display_average_rating(self, obj):
        if obj.average_rating_tenths is None:
            return '-'
        return obj.average_rating_tenths / 10


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'nconst',
        'name',
        'birth_year',
        'death_year',
        'primary_professions',
    )
    search_fields = ('nconst', 'name')


@admin.register(Principal)
class PrincipalAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'title',
        'person',
        'ordering',
        'display_category',
        'job',
        'characters',
    )
    search_fields = (
        'title__tconst',
        'title__title',
        'person__nconst',
        'person__name',
    )

    @admin.display(description='Категория')
    def display_category(self, obj):
        return obj.get_category_display()


@admin.register(Crew)
class CrewAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'title',
        'person',
        'display_role',
    )
    search_fields = (
        'title__tconst',
        'title__title',
        'person__nconst',
        'person__name',
    )

    @admin.display(description='Роль')
    def display_role(self, obj):
        return obj.get_role_display()