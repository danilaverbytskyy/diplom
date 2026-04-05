import csv
import datetime
import io
import json
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection

from main.models import Crew, Genre, Person, Principal, Rating, Title


TITLE_BATCH_SIZE = 50000
RATING_BATCH_SIZE = 50000
PERSON_BATCH_SIZE = 50000
RELATION_BATCH_SIZE = 100000

ALLOWED_TITLE_TYPES = {
    'movie',
    'short',
    'tvSeries',
    'tvMiniSeries',
    'tvMovie',
}

TITLE_TYPE_MAP = {
    'movie': 1,
    'short': 2,
    'tvSeries': 3,
    'tvMiniSeries': 4,
    'tvMovie': 5,
    'tvEpisode': 6,
    'tvShort': 7,
    'tvSpecial': 8,
    'tvPilot': 9,
    'video': 10,
    'videoGame': 11,
    'podcastSeries': 12,
    'podcastEpisode': 13,
    'radioSeries': 14,
    'radioEpisode': 15,
    'musicVideo': 16,
    'audiobook': 17,
    'other': 99,
}

CREW_ROLE_MAP = {
    'director': 1,
    'writer': 2,
}

PRINCIPAL_CATEGORY_MAP = {
    'actor': 1,
    'actress': 2,
    'director': 3,
    'writer': 4,
    'producer': 5,
    'composer': 6,
    'editor': 7,
    'cinematographer': 8,
    'self': 9,
    'archive_footage': 10,
    'archive_sound': 11,
    'soundtrack': 12,
    'assistant_director': 13,
    'casting_director': 14,
    'production_designer': 15,
    'art_director': 16,
    'costume_designer': 17,
    'make_up_department': 18,
    'camera_department': 19,
    'music_department': 20,
    'sound_department': 21,
    'visual_effects': 22,
    'animation_department': 23,
    'executive_producer': 24,
    'archive_material': 25,
    'other': 99,
}


def nullify(value: str):
    if value in (r'\N', '', None):
        return None
    return value


def to_int(value: str):
    value = nullify(value)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_bool(value: str):
    value = nullify(value)
    if value is None:
        return False
    return value == '1'


def split_csv_field(value: str):
    value = nullify(value)
    if value is None:
        return []
    return [item.strip() for item in value.split(',') if item.strip()]


def parse_characters_text(value: str):
    value = nullify(value)
    if value is None:
        return None

    if not (value.startswith('[') and value.endswith(']')):
        return value

    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return ', '.join(str(item) for item in parsed if item is not None)
        return str(parsed)
    except json.JSONDecodeError:
        return value


def rating_to_tenths(value: str):
    value = nullify(value)
    if value is None:
        return None
    try:
        return int(round(float(value) * 10))
    except (TypeError, ValueError):
        return None


def escape_copy_text(value):
    if value is None:
        return r'\N'

    return (
        str(value)
        .replace('\\', '\\\\')
        .replace('\t', '\\t')
        .replace('\n', '\\n')
        .replace('\r', '\\r')
    )


class Command(BaseCommand):
    help = 'Import IMDb TSV datasets into optimized models'

    def add_arguments(self, parser):
        parser.add_argument(
            '--path',
            type=str,
            default=str(Path(settings.BASE_DIR) / 'data' / 'imdb'),
            help='Path to directory with IMDb .tsv files',
        )
        parser.add_argument(
            '--truncate',
            action='store_true',
            help='Delete existing data before import',
        )
        parser.add_argument(
            '--skip-titles',
            action='store_true',
            help='Skip title.basics import',
        )
        parser.add_argument(
            '--skip-ratings',
            action='store_true',
            help='Skip title.ratings import',
        )
        parser.add_argument(
            '--skip-persons',
            action='store_true',
            help='Skip name.basics import',
        )
        parser.add_argument(
            '--skip-crew',
            action='store_true',
            help='Skip title.crew import',
        )
        parser.add_argument(
            '--skip-principals',
            action='store_true',
            help='Skip title.principals import',
        )

    def handle(self, *args, **options):
        base_path = Path(options['path'])

        if not base_path.exists():
            self.stderr.write(self.style.ERROR(f'Directory does not exist: {base_path}'))
            return

        # if options['truncate']:
        #     self.truncate_tables()
        #
        # if not options['skip_titles']:
        #     self.import_titles(base_path / 'title.basics.tsv')
        #
        # if not options['skip_ratings']:
        #     self.import_ratings(base_path / 'title.ratings.tsv')
        #
        # if not options['skip_persons']:
        #     self.import_persons(base_path / 'name.basics.tsv')
        #
        # if not options['skip_crew']:
        #     self.import_crew(base_path / 'title.crew.tsv')

        if not options['skip_principals']:
            self.import_principals(base_path / 'title.principals.tsv')

        self.stdout.write(self.style.SUCCESS('IMDb import completed'))

    def truncate_tables(self):
        self.stdout.write('Deleting existing data...')

        Principal.objects.all().delete()
        Crew.objects.all().delete()
        Rating.objects.all().delete()
        Person.objects.all().delete()
        Title.objects.all().delete()
        Genre.objects.all().delete()

        self.stdout.write(self.style.SUCCESS('Existing data deleted'))

    def open_file(self, filepath: Path):
        return open(filepath, mode='rt', encoding='utf-8')

    def copy_text_to_temp_table(self, copy_sql: str, payload: str):
        connection.ensure_connection()
        raw_conn = connection.connection

        with raw_conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                copy.write(payload)

    def import_titles(self, filepath: Path):
        self.stdout.write(f'Importing titles from {filepath.name}')

        titles_batch = []
        genre_names = set()
        count = 0
        skipped_type = 0
        skipped_bad_runtime = 0
        processed = 0

        with self.open_file(filepath) as f:
            reader = csv.DictReader(f, delimiter='\t')

            for row in reader:
                processed += 1

                raw_title_type = nullify(row['titleType']) or 'other'
                if raw_title_type not in ALLOWED_TITLE_TYPES:
                    skipped_type += 1
                    continue

                runtime_raw = row['runtimeMinutes']
                runtime_minutes = to_int(runtime_raw)
                if runtime_raw not in (None, '', r'\N') and runtime_minutes is None:
                    skipped_bad_runtime += 1
                    continue

                title_type_code = TITLE_TYPE_MAP.get(raw_title_type, 99)

                genres = split_csv_field(row['genres'])
                genre_names.update(genres)

                titles_batch.append(
                    Title(
                        tconst=row['tconst'],
                        title_type=title_type_code,
                        title=nullify(row['primaryTitle']) or '',
                        is_adult=to_bool(row['isAdult']),
                        start_year=to_int(row['startYear']),
                        end_year=to_int(row['endYear']),
                        runtime_minutes=runtime_minutes,
                    )
                )

                if len(titles_batch) >= TITLE_BATCH_SIZE:
                    Title.objects.bulk_create(titles_batch, ignore_conflicts=True)
                    count += len(titles_batch)
                    titles_batch = []
                    self.stdout.write(
                        f'Imported titles: {count}, skipped by type: {skipped_type}, skipped bad runtime: {skipped_bad_runtime}'
                    )

        if titles_batch:
            Title.objects.bulk_create(titles_batch, ignore_conflicts=True)
            count += len(titles_batch)

        self.stdout.write('Creating genres...')
        existing_genres = set(Genre.objects.values_list('name', flat=True))
        new_genres = [Genre(name=name) for name in genre_names if name not in existing_genres]
        if new_genres:
            Genre.objects.bulk_create(new_genres, ignore_conflicts=True)

        self.stdout.write('Linking titles and genres...')
        genre_map = dict(Genre.objects.values_list('name', 'id'))
        title_id_map = dict(Title.objects.values_list('tconst', 'id'))

        through_model = Title.genres.through
        relations_batch = []
        rel_count = 0

        with self.open_file(filepath) as f:
            reader = csv.DictReader(f, delimiter='\t')

            for row in reader:
                raw_title_type = nullify(row['titleType']) or 'other'
                if raw_title_type not in ALLOWED_TITLE_TYPES:
                    continue

                runtime_raw = row['runtimeMinutes']
                runtime_minutes = to_int(runtime_raw)
                if runtime_raw not in (None, '', r'\N') and runtime_minutes is None:
                    continue

                title_pk = title_id_map.get(row['tconst'])
                if not title_pk:
                    continue

                for genre_name in split_csv_field(row['genres']):
                    genre_id = genre_map.get(genre_name)
                    if genre_id:
                        relations_batch.append(
                            through_model(title_id=title_pk, genre_id=genre_id)
                        )

                if len(relations_batch) >= RELATION_BATCH_SIZE:
                    through_model.objects.bulk_create(relations_batch, ignore_conflicts=True)
                    rel_count += len(relations_batch)
                    relations_batch = []
                    self.stdout.write(f'Linked title-genre rows: {rel_count}')

        if relations_batch:
            through_model.objects.bulk_create(relations_batch, ignore_conflicts=True)
            rel_count += len(relations_batch)

        self.stdout.write(
            self.style.SUCCESS(
                f'Titles imported: {count}, title-genre links: {rel_count}, '
                f'skipped by type: {skipped_type}, skipped bad runtime: {skipped_bad_runtime}'
            )
        )

    def import_ratings(self, filepath: Path):
        self.stdout.write(f'Importing ratings from {filepath.name}')

        ratings_batch = []
        count = 0
        skipped_missing_title = 0

        title_id_map = dict(Title.objects.values_list('tconst', 'id'))

        with self.open_file(filepath) as f:
            reader = csv.DictReader(f, delimiter='\t')

            for row in reader:
                title_id = title_id_map.get(row['tconst'])
                if not title_id:
                    skipped_missing_title += 1
                    continue

                ratings_batch.append(
                    Rating(
                        title_id=title_id,
                        average_rating_tenths=rating_to_tenths(row['averageRating']),
                        num_votes=to_int(row['numVotes']) or 0,
                    )
                )

                if len(ratings_batch) >= RATING_BATCH_SIZE:
                    Rating.objects.bulk_create(
                        ratings_batch,
                        update_conflicts=True,
                        update_fields=['average_rating_tenths', 'num_votes'],
                        unique_fields=['title'],
                    )
                    count += len(ratings_batch)
                    ratings_batch = []
                    self.stdout.write(
                        f'Imported ratings: {count}, skipped missing title: {skipped_missing_title}'
                    )

        if ratings_batch:
            Rating.objects.bulk_create(
                ratings_batch,
                update_conflicts=True,
                update_fields=['average_rating_tenths', 'num_votes'],
                unique_fields=['title'],
            )
            count += len(ratings_batch)

        self.stdout.write(
            self.style.SUCCESS(
                f'Ratings imported: {count}, skipped missing title: {skipped_missing_title}'
            )
        )

    def import_persons(self, filepath: Path):
        self.stdout.write(f'Importing persons from {filepath.name}')

        persons_batch = []
        count = 0

        with self.open_file(filepath) as f:
            reader = csv.DictReader(f, delimiter='\t')

            for row in reader:
                professions = split_csv_field(row['primaryProfession'])
                professions_text = ','.join(professions) if professions else None

                persons_batch.append(
                    Person(
                        nconst=row['nconst'],
                        name=nullify(row['primaryName']) or '',
                        birth_year=to_int(row['birthYear']),
                        death_year=to_int(row['deathYear']),
                        primary_professions=professions_text,
                    )
                )

                if len(persons_batch) >= PERSON_BATCH_SIZE:
                    Person.objects.bulk_create(persons_batch, ignore_conflicts=True)
                    count += len(persons_batch)
                    persons_batch = []
                    self.stdout.write(f'Imported persons: {count}')

        if persons_batch:
            Person.objects.bulk_create(persons_batch, ignore_conflicts=True)
            count += len(persons_batch)

        self.stdout.write(self.style.SUCCESS(f'Persons imported: {count}'))

    def import_crew(self, filepath: Path):
        self.stdout.write(f'Importing crew from {filepath.name}')

        crew_table = Crew._meta.db_table
        title_table = Title._meta.db_table
        person_table = Person._meta.db_table

        processed_rows = 0
        inserted_total = 0
        skipped_empty = 0

        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS temp_crew')
            cursor.execute("""
                CREATE TEMP TABLE temp_crew (
                    title_tconst varchar(16),
                    person_nconst varchar(16),
                    role_code smallint
                )
            """)

        buffer = io.StringIO()
        rows_in_buffer = 0

        def flush_buffer():
            nonlocal buffer, rows_in_buffer, inserted_total

            if rows_in_buffer == 0:
                return

            payload = buffer.getvalue()

            self.copy_text_to_temp_table(
                '''
                COPY temp_crew
                (title_tconst, person_nconst, role_code)
                FROM STDIN WITH (FORMAT text, NULL '\\N')
                ''',
                payload,
            )

            with connection.cursor() as cursor:
                cursor.execute(f'''
                    INSERT INTO {crew_table}
                    (title_id, person_id, role)
                    SELECT
                        t.id,
                        p.id,
                        c.role_code
                    FROM temp_crew c
                    INNER JOIN {title_table} t ON t.tconst = c.title_tconst
                    INNER JOIN {person_table} p ON p.nconst = c.person_nconst
                    ON CONFLICT DO NOTHING
                ''')
                inserted_total += max(cursor.rowcount, 0)
                cursor.execute('TRUNCATE temp_crew')

            buffer = io.StringIO()
            rows_in_buffer = 0

        with self.open_file(filepath) as f:
            reader = csv.DictReader(f, delimiter='\t')

            for row in reader:
                processed_rows += 1

                title_tconst = row['tconst']
                directors = split_csv_field(row['directors'])
                writers = split_csv_field(row['writers'])

                if not directors and not writers:
                    skipped_empty += 1
                    continue

                for person_nconst in directors:
                    buffer.write(
                        f'{escape_copy_text(title_tconst)}\t'
                        f'{escape_copy_text(person_nconst)}\t'
                        f'{CREW_ROLE_MAP["director"]}\n'
                    )
                    rows_in_buffer += 1

                for person_nconst in writers:
                    buffer.write(
                        f'{escape_copy_text(title_tconst)}\t'
                        f'{escape_copy_text(person_nconst)}\t'
                        f'{CREW_ROLE_MAP["writer"]}\n'
                    )
                    rows_in_buffer += 1

                if rows_in_buffer >= RELATION_BATCH_SIZE:
                    flush_buffer()
                    self.stdout.write(
                        f'Processed source rows: {processed_rows}, inserted crew rows: {inserted_total}, skipped empty rows: {skipped_empty}'
                    )

        flush_buffer()

        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS temp_crew')

        self.stdout.write(
            self.style.SUCCESS(
                f'Crew imported. Processed source rows: {processed_rows}, '
                f'inserted crew rows: {inserted_total}, skipped empty rows: {skipped_empty}'
            )
        )

    def import_principals(self, filepath: Path):
        self.stdout.write(f'Importing principals from {filepath.name}')

        principals_table = Principal._meta.db_table
        title_table = Title._meta.db_table
        person_table = Person._meta.db_table

        processed_rows = 0
        inserted_total = 0
        skipped_category = 0
        truncated_job = 0

        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS temp_principals')
            cursor.execute("""
                CREATE TEMP TABLE temp_principals (
                    title_tconst varchar(16),
                    person_nconst varchar(16),
                    ordering_int integer,
                    category_code smallint,
                    job varchar(255),
                    characters_text text
                )
            """)

        buffer = io.StringIO()
        rows_in_buffer = 0

        def flush_buffer():
            nonlocal buffer, rows_in_buffer, inserted_total

            if rows_in_buffer == 0:
                return

            payload = buffer.getvalue()

            self.copy_text_to_temp_table(
                '''
                COPY temp_principals
                (title_tconst, person_nconst, ordering_int, category_code, job, characters_text)
                FROM STDIN WITH (FORMAT text, NULL '\\N')
                ''',
                payload,
            )

            with connection.cursor() as cursor:
                cursor.execute(f'''
                    INSERT INTO {principals_table}
                    (title_id, person_id, ordering, category, job, characters)
                    SELECT
                        t.id,
                        p.id,
                        tp.ordering_int,
                        tp.category_code,
                        tp.job,
                        tp.characters_text
                    FROM temp_principals tp
                    INNER JOIN {title_table} t ON t.tconst = tp.title_tconst
                    INNER JOIN {person_table} p ON p.nconst = tp.person_nconst
                    ON CONFLICT DO NOTHING
                ''')
                inserted_total += max(cursor.rowcount, 0)
                cursor.execute('TRUNCATE temp_principals')

            buffer = io.StringIO()
            rows_in_buffer = 0

        with self.open_file(filepath) as f:
            reader = csv.DictReader(f, delimiter='\t')

            for row in reader:
                processed_rows += 1

                category_raw = nullify(row['category']) or 'other'
                category_code = PRINCIPAL_CATEGORY_MAP.get(category_raw)

                if category_code is None:
                    skipped_category += 1
                    category_code = PRINCIPAL_CATEGORY_MAP['other']

                characters_text = parse_characters_text(row['characters'])

                job = nullify(row['job'])
                if job is not None and len(job) > 255:
                    truncated_job += 1
                    job = job[:255]

                buffer.write(
                    f'{escape_copy_text(row["tconst"])}\t'
                    f'{escape_copy_text(row["nconst"])}\t'
                    f'{to_int(row["ordering"]) or 0}\t'
                    f'{category_code}\t'
                    f'{escape_copy_text(job)}\t'
                    f'{escape_copy_text(characters_text)}\n'
                )
                rows_in_buffer += 1

                if rows_in_buffer >= RELATION_BATCH_SIZE:
                    flush_buffer()
                    self.stdout.write(
                        f'Processed source rows: {processed_rows}, inserted principals: {inserted_total}, '
                        f'skipped unknown category: {skipped_category}, truncated job: {truncated_job}'
                    )

        flush_buffer()

        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS temp_principals')

        self.stdout.write(
            self.style.SUCCESS(
                f'Principals imported. Processed source rows: {processed_rows}, '
                f'inserted principals: {inserted_total}, skipped unknown category: {skipped_category}, '
                f'truncated job: {truncated_job}'
            )
        )