from django.db import models


class Genre(models.Model):
    name = models.CharField('Название', max_length=16, unique=True)

    class Meta:
        verbose_name = 'Жанр'
        verbose_name_plural = 'Жанры'

    def __str__(self):
        return self.name


class TitleType(models.TextChoices):
    MOVIE = 'movie', 'Фильм'
    SHORT = 'short', 'Короткометражка'
    TV_SERIES = 'tvSeries', 'Сериал'
    TV_MINI_SERIES = 'tvMiniSeries', 'Мини-сериал'
    TV_MOVIE = 'tvMovie', 'ТВ-фильм'
    TV_EPISODE = 'tvEpisode', 'Эпизод'
    TV_SHORT = 'tvShort', 'ТВ-короткометражка'
    TV_SPECIAL = 'tvSpecial', 'ТВ-спецвыпуск'
    TV_PILOT = 'tvPilot', 'ТВ-пилот'
    VIDEO = 'video', 'Видео'
    VIDEO_GAME = 'videoGame', 'Видеоигра'
    PODCAST_SERIES = 'podcastSeries', 'Подкаст-сериал'
    PODCAST_EPISODE = 'podcastEpisode', 'Эпизод подкаста'
    RADIO_SERIES = 'radioSeries', 'Радиосериал'
    RADIO_EPISODE = 'radioEpisode', 'Радиоэпизод'
    MUSIC_VIDEO = 'musicVideo', 'Музыкальное видео'
    AUDIOBOOK = 'audiobook', 'Аудиокнига'
    OTHER = 'other', 'Другое'


class CrewRole(models.TextChoices):
    DIRECTOR = 'director', 'Режиссер'
    WRITER = 'writer', 'Сценарист'


class Title(models.Model):
    tconst = models.CharField('IMDb ID', max_length=16, primary_key=True)
    title_type = models.CharField('Тип произведения', max_length=32)
    title = models.CharField('Основное название', max_length=512)
    is_adult = models.BooleanField('18+', default=False)
    start_year = models.PositiveSmallIntegerField('Год начала', null=True, blank=True)
    end_year = models.PositiveSmallIntegerField('Год окончания', null=True, blank=True)
    runtime_minutes = models.PositiveSmallIntegerField('Длительность (мин.)', null=True, blank=True)
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
    average_rating = models.DecimalField(
        'Средний рейтинг',
        max_digits=3,
        decimal_places=1,
        null=True,
        blank=True,
    )
    num_votes = models.PositiveIntegerField('Количество голосов', default=0)

    class Meta:
        verbose_name = 'Рейтинг'
        verbose_name_plural = 'Рейтинги'

    def __str__(self):
        return f'{self.title_id}: {self.average_rating} ({self.num_votes})'


class Person(models.Model):
    nconst = models.CharField('IMDb ID', max_length=16, primary_key=True)
    name = models.CharField('Имя', max_length=255)
    birth_year = models.PositiveSmallIntegerField('Год рождения', null=True, blank=True)
    death_year = models.PositiveSmallIntegerField('Год смерти', null=True, blank=True)
    primary_professions = models.JSONField('Профессии', default=list, blank=True)

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
    category = models.CharField('Категория', max_length=64)
    job = models.CharField('Должность', max_length=255, null=True, blank=True)
    characters = models.JSONField('Персонажи', null=True, blank=True)

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
    role = models.CharField('Роль', max_length=16, choices=CrewRole.choices)

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