import csv
import io
import json
from itertools import islice, count
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection

from main.models import Genre, Person, Title, Crew, Principal, Rating


BATCH_SIZE = 30000
PRINCIPALS_COPY_BATCH_ROWS = 100000


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


def parse_characters(value: str):
    value = nullify(value)
    if value is None:
        return None

    if not (value.startswith('[') and value.endswith(']')):
        return None

    try:
        return json.loads(value)
    except json.JSONDecodeError:
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
    help = 'Import IMDb datasets from TSV files'

    def add_arguments(self, parser):
        parser.add_argument(
            '--path',
            type=str,
            default=str(Path(settings.BASE_DIR) / 'data' / 'imdb'),
            help='Path to directory with IMDb .tsv files',
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
        parser.add_argument(
            '--truncate',
            action='store_true',
            help='Delete existing data before import',
        )

    def handle(self, *args, **options):
        base_path = Path(options['path'])

        if not base_path.exists():
            self.stderr.write(self.style.ERROR(f'Directory does not exist: {base_path}'))
            return

        # self.import_crew(base_path / 'title.crew.tsv')

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

        genre_names = set()
        titles_batch = []
        count = 0

        start_from = Title.objects.count()

        with self.open_file(filepath) as f:
            header_line = next(f).rstrip('\n')
            fieldnames = header_line.split('\t')

            skipped_iter = islice(f, start_from - 1, None)

            reader = csv.DictReader(
                skipped_iter,
                delimiter='\t',
                fieldnames=fieldnames,
            )

            for row in reader:
                genres = split_csv_field(row['genres'])
                genre_names.update(genres)

                runtime_raw = row['runtimeMinutes']
                runtime_minutes = to_int(runtime_raw)

                if runtime_raw not in (None, '', r'\N') and runtime_minutes is None:
                    continue

                titles_batch.append(
                    Title(
                        tconst=row['tconst'],
                        title_type=nullify(row['titleType']) or 'other',
                        primary_title=nullify(row['primaryTitle']) or '',
                        is_adult=to_bool(row['isAdult']),
                        start_year=to_int(row['startYear']),
                        end_year=to_int(row['endYear']),
                        runtime_minutes=runtime_minutes,
                    )
                )

                if len(titles_batch) >= BATCH_SIZE:
                    Title.objects.bulk_create(titles_batch, ignore_conflicts=True)
                    count += len(titles_batch)
                    self.stdout.write(f'Imported titles: {count}')
                    titles_batch = []

        if titles_batch:
            Title.objects.bulk_create(titles_batch, ignore_conflicts=True)
            count += len(titles_batch)

        self.stdout.write('Creating genres...')
        existing_genres = set(Genre.objects.values_list('name', flat=True))
        new_genres = [Genre(name=name) for name in genre_names if name not in existing_genres]
        if new_genres:
            Genre.objects.bulk_create(new_genres, ignore_conflicts=True)

        self.stdout.write('Linking titles and genres...')
        genre_map = {g.name: g for g in Genre.objects.all()}

        through_model = Title.genres.through
        relations_batch = []
        rel_count = 0

        with self.open_file(filepath) as f:
            reader = csv.DictReader(f, delimiter='\t')

            for row in reader:
                tconst = row['tconst']
                for genre_name in split_csv_field(row['genres']):
                    genre = genre_map.get(genre_name)
                    if genre:
                        relations_batch.append(
                            through_model(title_id=tconst, genre_id=genre.id)
                        )

                if len(relations_batch) >= BATCH_SIZE * 5:
                    through_model.objects.bulk_create(relations_batch, ignore_conflicts=True)
                    rel_count += len(relations_batch)
                    self.stdout.write(f'Linked title-genre rows: {rel_count}')
                    relations_batch = []

        if relations_batch:
            through_model.objects.bulk_create(relations_batch, ignore_conflicts=True)
            rel_count += len(relations_batch)

        self.stdout.write(
            self.style.SUCCESS(
                f'Titles imported: {count}, title-genre links: {rel_count}'
            )
        )

    def import_ratings(self, filepath: Path):
        self.stdout.write(f'Importing ratings from {filepath.name}')

        ratings_batch = []
        count = 0

        with self.open_file(filepath) as f:
            start_from = Rating.objects.count()
            header_line = next(f).rstrip('\n')
            fieldnames = header_line.split('\t')

            skipped_iter = islice(f, start_from - 1, None)

            reader = csv.DictReader(
                skipped_iter,
                delimiter='\t',
                fieldnames=fieldnames,
            )

            for row in reader:
                if row['tconst'] in {'tt12149332', 'tt27404292', 'tt28535095', 'tt3984412'}:
                    continue

                ratings_batch.append(
                    Rating(
                        title_id=row['tconst'],
                        average_rating=nullify(row['averageRating']),
                        num_votes=to_int(row['numVotes']) or 0,
                    )
                )

                if len(ratings_batch) >= BATCH_SIZE:
                    Rating.objects.bulk_create(
                        ratings_batch,
                        update_conflicts=True,
                        update_fields=['average_rating', 'num_votes'],
                        unique_fields=['title'],
                    )
                    count += len(ratings_batch)
                    self.stdout.write(f'Imported ratings: {count}')
                    ratings_batch = []

        if ratings_batch:
            Rating.objects.bulk_create(
                ratings_batch,
                update_conflicts=True,
                update_fields=['average_rating', 'num_votes'],
                unique_fields=['title'],
            )
            count += len(ratings_batch)

        self.stdout.write(self.style.SUCCESS(f'Ratings imported: {count}'))

    def import_persons(self, filepath: Path):
        self.stdout.write(f'Importing persons from {filepath.name}')

        persons_batch = []
        count = 0

        with self.open_file(filepath) as f:
            reader = csv.DictReader(f, delimiter='\t')

            for row in reader:
                persons_batch.append(
                    Person(
                        nconst=row['nconst'],
                        primary_name=nullify(row['primaryName']) or '',
                        birth_year=to_int(row['birthYear']),
                        death_year=to_int(row['deathYear']),
                        primary_professions=split_csv_field(row['primaryProfession']),
                    )
                )

                if len(persons_batch) >= BATCH_SIZE:
                    Person.objects.bulk_create(persons_batch, ignore_conflicts=True)
                    count += len(persons_batch)
                    self.stdout.write(f'Imported persons: {count}')
                    persons_batch = []

        if persons_batch:
            Person.objects.bulk_create(persons_batch, ignore_conflicts=True)
            count += len(persons_batch)

        self.stdout.write(self.style.SUCCESS(f'Persons imported: {count}'))

    def import_crew(self, filepath: Path):
        self.stdout.write(f'Importing crew from {filepath.name}')

        crew_batch = []
        count = 0

        valid_titles = set(Title.objects.values_list('tconst', flat=True))
        valid_persons = set(Person.objects.values_list('nconst', flat=True))

        with self.open_file(filepath) as f:
            reader = csv.DictReader(f, delimiter='\t')

            for row in reader:
                tconst = row['tconst']
                if tconst not in valid_titles:
                    continue

                for director_id in split_csv_field(row['directors']):
                    if director_id in valid_persons:
                        crew_batch.append(
                            Crew(
                                title_id=tconst,
                                person_id=director_id,
                                role='director',
                            )
                        )

                for writer_id in split_csv_field(row['writers']):
                    if writer_id in valid_persons:
                        crew_batch.append(
                            Crew(
                                title_id=tconst,
                                person_id=writer_id,
                                role='writer',
                            )
                        )

                if len(crew_batch) >= BATCH_SIZE * 5:
                    Crew.objects.bulk_create(crew_batch, ignore_conflicts=True)
                    count += len(crew_batch)
                    self.stdout.write(f'Imported crew rows: {count}')
                    crew_batch = []

        if crew_batch:
            Crew.objects.bulk_create(crew_batch, ignore_conflicts=True)
            count += len(crew_batch)

        self.stdout.write(self.style.SUCCESS(f'Crew imported: {count}'))

    def import_principals(self, filepath: Path):
        self.stdout.write(f'Importing principals from {filepath.name}')

        principals_table = Principal._meta.db_table
        person_table = Person._meta.db_table
        title_table = Title._meta.db_table

        processed = 0
        inserted_total = 0
        batch_rows = PRINCIPALS_COPY_BATCH_ROWS

        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS temp_title_principals')
            cursor.execute("""
                CREATE TEMP TABLE temp_title_principals (
                    title_id varchar(16),
                    person_id varchar(16),
                    ordering_int integer,
                    category varchar(64),
                    job varchar(255),
                    characters_json jsonb
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
                COPY temp_title_principals
                (title_id, person_id, ordering_int, category, job, characters_json)
                FROM STDIN WITH (FORMAT text, NULL '\\N')
                ''',
                payload,
            )

            with connection.cursor() as cursor:
                cursor.execute(f'''
                    INSERT INTO {principals_table}
                    (title_id, person_id, ordering, category, job, characters)
                    SELECT
                        t.title_id,
                        t.person_id,
                        t.ordering_int,
                        t.category,
                        t.job,
                        t.characters_json
                    FROM temp_title_principals t
                    INNER JOIN {title_table} ti ON ti.tconst = t.title_id
                    INNER JOIN {person_table} p ON p.nconst = t.person_id
                    ON CONFLICT DO NOTHING
                ''')
                inserted_total += max(cursor.rowcount, 0)
                cursor.execute('TRUNCATE temp_title_principals')

            buffer = io.StringIO()
            rows_in_buffer = 0

        with self.open_file(filepath) as f:
            reader = csv.DictReader(f, delimiter='\t')
            my_count = 89_399_310
            for row in reader:
                processed += 1
                if processed < my_count:
                    if processed % 1_000_000 == 0:
                        print(f'skipped {processed}')
                    continue

                title_id = row['tconst']
                person_id = row['nconst']
                ordering_value = to_int(row['ordering']) or 0
                category = nullify(row['category']) or 'other'
                job = nullify(row['job'])
                characters = parse_characters(row['characters'])

                buffer.write(
                    f'{escape_copy_text(title_id)}\t'
                    f'{escape_copy_text(person_id)}\t'
                    f'{ordering_value}\t'
                    f'{escape_copy_text(category)}\t'
                    f'{escape_copy_text(job)}\t'
                    f'{escape_copy_text(json.dumps(characters, ensure_ascii=False) if characters is not None else None)}\n'
                )
                rows_in_buffer += 1

                if rows_in_buffer >= batch_rows:
                    flush_buffer()
                    self.stdout.write(
                        f'Processed rows: {processed}, inserted principals: {inserted_total}, {round(processed / 98694691 * 100, 1)}%'
                    )

                    if processed > 100_000_000:
                        print('слишком огромная таблица')
                        return

        flush_buffer()

        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS temp_title_principals')

        self.stdout.write(
            self.style.SUCCESS(
                f'Principals imported. Processed rows: {processed}, inserted: {inserted_total}, {round(processed / 98694691 * 100, 1)}%'
            )
        )

        if processed > 100_000_000:
            print('слишком огромная таблица')
            return
