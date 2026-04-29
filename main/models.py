from django.db import models


class Genre(models.Model):
    name = models.CharField('Название', max_length=16, unique=True)

    class Meta:
        verbose_name = 'Жанр'
        verbose_name_plural = 'Жанры'

    def __str__(self):
        return self.name


class TitleType(models.IntegerChoices):
    MOVIE = 1, 'Фильм'
    SHORT = 2, 'Короткометражка'
    TV_SERIES = 3, 'Сериал'
    TV_MINI_SERIES = 4, 'Мини-сериал'
    TV_MOVIE = 5, 'ТВ-фильм'
    OTHER = 99, 'Другое'


class CrewRole(models.IntegerChoices):
    DIRECTOR = 1, 'Режиссер'
    WRITER = 2, 'Сценарист'
    OTHER = 99, 'Другое'


class PrincipalCategory(models.IntegerChoices):
    ACTOR = 1, 'Актер'
    ACTRESS = 2, 'Актриса'
    DIRECTOR = 3, 'Режиссер'
    WRITER = 4, 'Сценарист'
    PRODUCER = 5, 'Продюсер'
    COMPOSER = 6, 'Композитор'
    EDITOR = 7, 'Монтажер'
    CINEMATOGRAPHER = 8, 'Оператор'
    SELF = 9, 'Играет себя'
    ARCHIVE_FOOTAGE = 10, 'Архивные кадры'
    ARCHIVE_SOUND = 11, 'Архивный звук'
    SOUNDTRACK = 12, 'Саундтрек'
    ASSISTANT_DIRECTOR = 13, 'Помощник режиссера'
    CASTING_DIRECTOR = 14, 'Кастинг-директор'
    PRODUCTION_DESIGNER = 15, 'Художник-постановщик'
    OTHER = 99, 'Другое'


class Title(models.Model):
    id = models.BigAutoField(primary_key=True)
    tconst = models.CharField('IMDb ID', max_length=16, unique=True)
    title_type = models.PositiveSmallIntegerField('Тип произведения', choices=TitleType.choices)
    title = models.CharField('Основное название', max_length=512)
    is_adult = models.BooleanField('18+', default=False)
    start_year = models.PositiveSmallIntegerField('Год начала', null=True, blank=True)
    end_year = models.PositiveSmallIntegerField('Год окончания', null=True, blank=True)
    runtime_minutes = models.PositiveIntegerField('Длительность (мин.)', null=True, blank=True)
    genres = models.ManyToManyField(
        Genre,
        verbose_name='Жанры',
        related_name='titles',
        blank=True,
    )

    class Meta:
        verbose_name = 'Произведение'
        verbose_name_plural = 'Произведения'

    def __str__(self):
        year = self.start_year if self.start_year else 'N/A'
        return f'{self.title} ({year})'


class Rating(models.Model):
    title = models.OneToOneField(
        Title,
        verbose_name='Произведение',
        on_delete=models.CASCADE,
        related_name='rating',
        primary_key=True,
    )
    average_rating_tenths = models.PositiveSmallIntegerField(
        'Средний рейтинг x10',
        null=True,
        blank=True,
    )
    num_votes = models.PositiveIntegerField('Количество голосов', default=0)

    class Meta:
        verbose_name = 'Рейтинг'
        verbose_name_plural = 'Рейтинги'

    def __str__(self):
        if self.average_rating_tenths is None:
            return f'{self.title_id}: None ({self.num_votes})'
        return f'{self.title_id}: {self.average_rating_tenths / 10} ({self.num_votes})'


class Person(models.Model):
    id = models.BigAutoField(primary_key=True)
    nconst = models.CharField('IMDb ID', max_length=16, unique=True)
    name = models.CharField('Имя', max_length=255)
    birth_year = models.PositiveSmallIntegerField('Год рождения', null=True, blank=True)
    death_year = models.PositiveSmallIntegerField('Год смерти', null=True, blank=True)
    primary_professions = models.TextField('Профессии', null=True, blank=True)

    class Meta:
        verbose_name = 'Персона'
        verbose_name_plural = 'Персоны'

    def __str__(self):
        return self.name


class Principal(models.Model):
    title = models.ForeignKey(
        Title,
        verbose_name='Произведение',
        on_delete=models.CASCADE,
        related_name='principals',
    )
    person = models.ForeignKey(
        Person,
        verbose_name='Персона',
        on_delete=models.CASCADE,
        related_name='principal_titles',
    )
    ordering = models.PositiveIntegerField('Порядок')
    category = models.PositiveSmallIntegerField('Категория', choices=PrincipalCategory.choices)
    job = models.CharField('Должность', max_length=255, null=True, blank=True)
    characters = models.TextField('Персонажи', null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['title', 'ordering'],
                name='unique_principal_ordering_per_title',
            ),
            models.UniqueConstraint(
                fields=['title', 'person', 'category', 'ordering'],
                name='unique_title_person_category_ordering',
            ),
        ]
        verbose_name = 'Основной участник'
        verbose_name_plural = 'Основные участники'

    def __str__(self):
        return f'{self.title_id} - {self.person_id} - {self.category}'


class Crew(models.Model):
    title = models.ForeignKey(
        Title,
        verbose_name='Произведение',
        on_delete=models.CASCADE,
        related_name='crew_members',
    )
    person = models.ForeignKey(
        Person,
        verbose_name='Персона',
        on_delete=models.CASCADE,
        related_name='crew_titles',
    )
    role = models.PositiveSmallIntegerField('Роль', choices=CrewRole.choices)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['title', 'person', 'role'],
                name='unique_title_person_role',
            )
        ]
        verbose_name = 'Участник съемочной группы'
        verbose_name_plural = 'Участники съемочной группы'

    def __str__(self):
        return f'{self.title_id} - {self.person_id} - {self.role}'