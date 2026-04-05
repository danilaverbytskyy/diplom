import csv
import io
from itertools import islice
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connection

from main.models import Person, Title, Crew


CREW_COPY_BATCH_ROWS = 100000


def nullify(value: str):
    if value in (r'\N', '', None):
        return None
    return value


def split_csv_field(value: str):
    value = nullify(value)
    if value is None:
        return []
    return [item.strip() for item in value.split(',') if item.strip()]


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
    help = 'Fast import of IMDb title.crew.tsv into TitleCrew'

    def add_arguments(self, parser):
        parser.add_argument(
            '--path',
            type=str,
            default=str(Path(settings.BASE_DIR) / 'data' / 'imdb'),
            help='Path to directory with IMDb .tsv files',
        )
        parser.add_argument(
            '--start-from',
            type=int,
            default=1,
            help='Start reading title.crew.tsv from this data row number (1-based)',
        )
        parser.add_argument(
            '--truncate-crew',
            action='store_true',
            help='Delete existing TitleCrew data before import',
        )

    def handle(self, *args, **options):
        base_path = Path(options['path'])
        crew_path = base_path / 'title.crew.tsv'
        start_from = Crew.objects.count()

        if not base_path.exists():
            self.stderr.write(self.style.ERROR(f'Directory does not exist: {base_path}'))
            return

        if not crew_path.exists():
            self.stderr.write(self.style.ERROR(f'File does not exist: {crew_path}'))
            return

        if options['truncate_crew']:
            self.truncate_crew()

        self.import_crew(crew_path, start_from=start_from)
        self.stdout.write(self.style.SUCCESS('IMDb crew import completed'))

    def truncate_crew(self):
        self.stdout.write('Deleting existing TitleCrew data...')
        Crew.objects.all().delete()
        self.stdout.write(self.style.SUCCESS('Existing TitleCrew data deleted'))

    def open_file(self, filepath: Path):
        return open(filepath, mode='rt', encoding='utf-8')

    def copy_text_to_temp_table(self, copy_sql: str, payload: str):
        connection.ensure_connection()
        raw_conn = connection.connection

        with raw_conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                copy.write(payload)

    def import_crew(self, filepath: Path, start_from: int = 1):
        self.stdout.write(f'Importing crew from {filepath.name}, start_from={start_from}')

        crew_table = Crew._meta.db_table
        title_table = Title._meta.db_table
        person_table = Person._meta.db_table

        processed_rows = 0
        inserted_total = 0
        skipped_empty = 0
        rows_in_buffer = 0
        batch_rows = CREW_COPY_BATCH_ROWS

        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS temp_title_crew')
            cursor.execute("""
                CREATE TEMP TABLE temp_title_crew (
                    title_id varchar(16),
                    person_id varchar(16),
                    role varchar(16)
                )
            """)

        buffer = io.StringIO()

        def flush_buffer():
            nonlocal buffer, rows_in_buffer, inserted_total

            if rows_in_buffer == 0:
                return

            payload = buffer.getvalue()

            self.copy_text_to_temp_table(
                '''
                COPY temp_title_crew
                (title_id, person_id, role)
                FROM STDIN WITH (FORMAT text, NULL '\\N')
                ''',
                payload,
            )

            with connection.cursor() as cursor:
                cursor.execute(f'''
                    INSERT INTO {crew_table}
                    (title_id, person_id, role)
                    SELECT
                        t.title_id,
                        t.person_id,
                        t.role
                    FROM temp_title_crew t
                    INNER JOIN {title_table} ti ON ti.tconst = t.title_id
                    INNER JOIN {person_table} p ON p.nconst = t.person_id
                    ON CONFLICT DO NOTHING
                ''')
                inserted_total += max(cursor.rowcount, 0)
                cursor.execute('TRUNCATE temp_title_crew')

            buffer = io.StringIO()
            rows_in_buffer = 0

        with self.open_file(filepath) as f:
            header_line = next(f).rstrip('\n')
            fieldnames = header_line.split('\t')

            data_iter = islice(f, start_from - 1, None) if start_from > 1 else f
            reader = csv.DictReader(
                data_iter,
                delimiter='\t',
                fieldnames=fieldnames,
            )

            for row in reader:
                processed_rows += 1

                title_id = row['tconst']
                directors = split_csv_field(row['directors'])
                writers = split_csv_field(row['writers'])

                if not directors and not writers:
                    skipped_empty += 1
                    continue

                for person_id in directors:
                    buffer.write(
                        f'{escape_copy_text(title_id)}\t'
                        f'{escape_copy_text(person_id)}\t'
                        f'director\n'
                    )
                    rows_in_buffer += 1

                for person_id in writers:
                    buffer.write(
                        f'{escape_copy_text(title_id)}\t'
                        f'{escape_copy_text(person_id)}\t'
                        f'writer\n'
                    )
                    rows_in_buffer += 1

                if rows_in_buffer >= batch_rows:
                    flush_buffer()
                    absolute_row = start_from + processed_rows - 1
                    self.stdout.write(
                        f'Processed source rows: {absolute_row}, '
                        f'inserted crew rows: {inserted_total}, '
                        f'skipped empty rows: {skipped_empty}'
                    )

        flush_buffer()

        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS temp_title_crew')

        self.stdout.write(
            self.style.SUCCESS(
                f'Crew imported. Processed source rows: {processed_rows}, '
                f'inserted crew rows: {inserted_total}, '
                f'skipped empty rows: {skipped_empty}'
            )
        )