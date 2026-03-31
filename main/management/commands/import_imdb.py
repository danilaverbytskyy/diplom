import csv
import gzip
import json
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from main.models import Genre, Person, Title, TitleCrew, TitlePrincipal, TitleRating


BATCH_SIZE = 100000


def nullify(value: str):
    if value == r"\N" or value == "":
        return None
    return value


def to_int(value: str):
    value = nullify(value)
    return int(value) if value is not None else None


def to_bool(value: str):
    value = nullify(value)
    if value is None:
        return False
    return value == "1"


def split_csv_field(value: str):
    value = nullify(value)
    if value is None:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_characters(value: str):
    value = nullify(value)
    if value is None:
        return None

    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return [value]


class Command(BaseCommand):
    help = "Import IMDb datasets from TSV.GZ files"

    def add_arguments(self, parser):
        parser.add_argument(
            "--path",
            type=str,
            default=str(Path(settings.BASE_DIR) / "data" / "imdb"),
            help="Path to directory with IMDb .tsv.gz files",
        )
        parser.add_argument(
            "--skip-titles",
            action="store_true",
            help="Skip title.basics import",
        )
        parser.add_argument(
            "--skip-ratings",
            action="store_true",
            help="Skip title.ratings import",
        )
        parser.add_argument(
            "--skip-persons",
            action="store_true",
            help="Skip name.basics import",
        )
        parser.add_argument(
            "--skip-known-for",
            action="store_true",
            help="Skip known_for_titles relations import",
        )
        parser.add_argument(
            "--skip-crew",
            action="store_true",
            help="Skip title.crew import",
        )
        parser.add_argument(
            "--skip-principals",
            action="store_true",
            help="Skip title.principals import",
        )
        parser.add_argument(
            "--truncate",
            action="store_true",
            help="Delete existing data before import",
        )

    def handle(self, *args, **options):
        base_path = Path(options["path"])

        if not base_path.exists():
            self.stderr.write(self.style.ERROR(f"Directory does not exist: {base_path}"))
            return

        if options["truncate"]:
            self.truncate_tables()

        if not options["skip_titles"]:
            self.import_titles(base_path / "title.basics.tsv.gz")

        if not options["skip_ratings"]:
            self.import_ratings(base_path / "title.ratings.tsv.gz")

        if not options["skip_persons"]:
            self.import_persons(base_path / "name.basics.tsv.gz")

        if not options["skip_known_for"]:
            self.import_known_for_titles(base_path / "name.basics.tsv.gz")

        if not options["skip_crew"]:
            self.import_crew(base_path / "title.crew.tsv.gz")

        if not options["skip_principals"]:
            self.import_principals(base_path / "title.principals.tsv.gz")

        self.stdout.write(self.style.SUCCESS("IMDb import completed"))

    def truncate_tables(self):
        self.stdout.write("Deleting existing data...")

        TitlePrincipal.objects.all().delete()
        TitleCrew.objects.all().delete()
        TitleRating.objects.all().delete()
        Person.objects.all().delete()
        Title.objects.all().delete()
        Genre.objects.all().delete()

        self.stdout.write(self.style.SUCCESS("Existing data deleted"))

    def open_tsv_gz(self, filepath: Path):
        return gzip.open(filepath, mode="rt", encoding="utf-8")

    def import_titles(self, filepath: Path):
        self.stdout.write(f"Importing titles from {filepath.name}")

        genre_names = set()
        titles_batch = []
        count = 0

        with self.open_tsv_gz(filepath) as f:
            reader = csv.DictReader(f, delimiter="\t")

            for row in reader:
                genres = split_csv_field(row["genres"])
                genre_names.update(genres)

                runtime_raw = row["runtimeMinutes"]

                try:
                    runtime_minutes = int(runtime_raw) if runtime_raw != r"\N" else None
                except ValueError:
                    continue

                titles_batch.append(
                    Title(
                        tconst=row["tconst"],
                        title_type=nullify(row["titleType"]) or "other",
                        primary_title=nullify(row["primaryTitle"]) or "",
                        original_title=nullify(row["originalTitle"]) or "",
                        is_adult=to_bool(row["isAdult"]),
                        start_year=to_int(row["startYear"]),
                        end_year=to_int(row["endYear"]),
                        runtime_minutes=runtime_minutes,
                    )
                )

                if len(titles_batch) >= BATCH_SIZE:
                    Title.objects.bulk_create(titles_batch, ignore_conflicts=True)
                    count += len(titles_batch)
                    self.stdout.write(f"Imported titles: {count}")
                    titles_batch = []

        if titles_batch:
            Title.objects.bulk_create(titles_batch, ignore_conflicts=True)
            count += len(titles_batch)

        self.stdout.write("Creating genres...")
        existing_genres = set(Genre.objects.values_list("name", flat=True))
        new_genres = [Genre(name=name) for name in genre_names if name not in existing_genres]
        Genre.objects.bulk_create(new_genres, ignore_conflicts=True)

        self.stdout.write("Linking titles and genres...")
        genre_map = {g.name: g for g in Genre.objects.all()}

        through_model = Title.genres.through
        relations_batch = []
        rel_count = 0

        with self.open_tsv_gz(filepath) as f:
            reader = csv.DictReader(f, delimiter="\t")

            for row in reader:
                tconst = row["tconst"]
                for genre_name in split_csv_field(row["genres"]):
                    genre = genre_map.get(genre_name)
                    if genre:
                        relations_batch.append(
                            through_model(title_id=tconst, genre_id=genre.id)
                        )

                if len(relations_batch) >= BATCH_SIZE * 5:
                    through_model.objects.bulk_create(relations_batch, ignore_conflicts=True)
                    rel_count += len(relations_batch)
                    self.stdout.write(f"Linked title-genre rows: {rel_count}")
                    relations_batch = []

        if relations_batch:
            through_model.objects.bulk_create(relations_batch, ignore_conflicts=True)
            rel_count += len(relations_batch)

        self.stdout.write(self.style.SUCCESS(f"Titles imported: {count}, title-genre links: {rel_count}"))

    def import_ratings(self, filepath: Path):
        self.stdout.write(f"Importing ratings from {filepath.name}")

        ratings_batch = []
        count = 0

        with self.open_tsv_gz(filepath) as f:
            reader = csv.DictReader(f, delimiter="\t")

            for row in reader:
                ratings_batch.append(
                    TitleRating(
                        title_id=row["tconst"],
                        average_rating=nullify(row["averageRating"]),
                        num_votes=to_int(row["numVotes"]) or 0,
                    )
                )

                if len(ratings_batch) >= BATCH_SIZE:
                    TitleRating.objects.bulk_create(
                        ratings_batch,
                        ignore_conflicts=True,
                        update_conflicts=True,
                        update_fields=["average_rating", "num_votes"],
                        unique_fields=["title"],
                    )
                    count += len(ratings_batch)
                    self.stdout.write(f"Imported ratings: {count}")
                    ratings_batch = []

        if ratings_batch:
            TitleRating.objects.bulk_create(
                ratings_batch,
                ignore_conflicts=True,
                update_conflicts=True,
                update_fields=["average_rating", "num_votes"],
                unique_fields=["title"],
            )
            count += len(ratings_batch)

        self.stdout.write(self.style.SUCCESS(f"Ratings imported: {count}"))

    def import_persons(self, filepath: Path):
        self.stdout.write(f"Importing persons from {filepath.name}")

        persons_batch = []
        count = 0

        with self.open_tsv_gz(filepath) as f:
            reader = csv.DictReader(f, delimiter="\t")

            for row in reader:
                persons_batch.append(
                    Person(
                        nconst=row["nconst"],
                        primary_name=nullify(row["primaryName"]) or "",
                        birth_year=to_int(row["birthYear"]),
                        death_year=to_int(row["deathYear"]),
                        primary_professions=split_csv_field(row["primaryProfession"]),
                    )
                )

                if len(persons_batch) >= BATCH_SIZE:
                    Person.objects.bulk_create(persons_batch, ignore_conflicts=True)
                    count += len(persons_batch)
                    self.stdout.write(f"Imported persons: {count}")
                    persons_batch = []

        if persons_batch:
            Person.objects.bulk_create(persons_batch, ignore_conflicts=True)
            count += len(persons_batch)

        self.stdout.write(self.style.SUCCESS(f"Persons imported: {count}"))

    def import_known_for_titles(self, filepath: Path):
        self.stdout.write(f"Importing known_for_titles from {filepath.name}")

        through_model = Person.known_for_titles.through
        relations_batch = []
        count = 0

        valid_titles = set(Title.objects.values_list("tconst", flat=True))
        valid_persons = set(Person.objects.values_list("nconst", flat=True))

        with self.open_tsv_gz(filepath) as f:
            reader = csv.DictReader(f, delimiter="\t")

            for row in reader:
                nconst = row["nconst"]

                if nconst not in valid_persons:
                    continue

                title_ids = split_csv_field(row["knownForTitles"])
                for tconst in title_ids:
                    if tconst in valid_titles:
                        relations_batch.append(
                            through_model(person_id=nconst, title_id=tconst)
                        )

                if len(relations_batch) >= BATCH_SIZE * 5:
                    through_model.objects.bulk_create(relations_batch, ignore_conflicts=True)
                    count += len(relations_batch)
                    self.stdout.write(f"Imported known_for links: {count}")
                    relations_batch = []

        if relations_batch:
            through_model.objects.bulk_create(relations_batch, ignore_conflicts=True)
            count += len(relations_batch)

        self.stdout.write(self.style.SUCCESS(f"Known-for links imported: {count}"))

    def import_crew(self, filepath: Path):
        self.stdout.write(f"Importing crew from {filepath.name}")

        crew_batch = []
        count = 0

        valid_titles = set(Title.objects.values_list("tconst", flat=True))
        valid_persons = set(Person.objects.values_list("nconst", flat=True))

        with self.open_tsv_gz(filepath) as f:
            reader = csv.DictReader(f, delimiter="\t")

            for row in reader:
                tconst = row["tconst"]
                if tconst not in valid_titles:
                    continue

                for director_id in split_csv_field(row["directors"]):
                    if director_id in valid_persons:
                        crew_batch.append(
                            TitleCrew(
                                title_id=tconst,
                                person_id=director_id,
                                role="director",
                            )
                        )

                for writer_id in split_csv_field(row["writers"]):
                    if writer_id in valid_persons:
                        crew_batch.append(
                            TitleCrew(
                                title_id=tconst,
                                person_id=writer_id,
                                role="writer",
                            )
                        )

                if len(crew_batch) >= BATCH_SIZE * 5:
                    TitleCrew.objects.bulk_create(crew_batch, ignore_conflicts=True)
                    count += len(crew_batch)
                    self.stdout.write(f"Imported crew rows: {count}")
                    crew_batch = []

        if crew_batch:
            TitleCrew.objects.bulk_create(crew_batch, ignore_conflicts=True)
            count += len(crew_batch)

        self.stdout.write(self.style.SUCCESS(f"Crew imported: {count}"))

    def import_principals(self, filepath: Path):
        self.stdout.write(f"Importing principals from {filepath.name}")

        principals_batch = []
        count = 0

        valid_titles = set(Title.objects.values_list("tconst", flat=True))
        valid_persons = set(Person.objects.values_list("nconst", flat=True))

        with self.open_tsv_gz(filepath) as f:
            reader = csv.DictReader(f, delimiter="\t")

            for row in reader:
                tconst = row["tconst"]
                nconst = row["nconst"]

                if tconst not in valid_titles or nconst not in valid_persons:
                    continue

                principals_batch.append(
                    TitlePrincipal(
                        title_id=tconst,
                        person_id=nconst,
                        ordering=to_int(row["ordering"]) or 0,
                        category=nullify(row["category"]) or "other",
                        job=nullify(row["job"]),
                        characters=parse_characters(row["characters"]),
                    )
                )

                if len(principals_batch) >= BATCH_SIZE:
                    TitlePrincipal.objects.bulk_create(principals_batch, ignore_conflicts=True)
                    count += len(principals_batch)
                    self.stdout.write(f"Imported principals: {count}")
                    principals_batch = []

        if principals_batch:
            TitlePrincipal.objects.bulk_create(principals_batch, ignore_conflicts=True)
            count += len(principals_batch)

        self.stdout.write(self.style.SUCCESS(f"Principals imported: {count}"))