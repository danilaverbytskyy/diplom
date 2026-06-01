# IMDb Titles API

Django/DRF-проект для работы с данными IMDb. В проекте есть:

- API для фильтрации, поиска и просмотра карточек тайтлов и персон;
- аналитика по жанрам и топ-тайтлам;
- многоуровневый кеш с режимами `off`, `local`, `redis`, `multi`;
- команды для импорта TSV-датасетов IMDb в PostgreSQL;
- Swagger / Redoc документация.

## Необходимое ПО

Для запуска через Docker:

- Docker 24+;
- Docker Compose;
- свободные порты `8000`, `5433`, `6379`.

Для локального запуска без Docker дополнительно понадобятся:

- Python 3.12+
- PostgreSQL 16
- Redis 7

## Подготовка окружения

В корне проекта должен быть файл `.env`.

Пример:

```env
DB_NAME=myapp
DB_USER=myapp
DB_PASSWORD=myapp
DB_HOST=db
DB_PORT=5432
REDIS_URL=redis://redis:6379/1
CACHE_MODE=off
CACHE_PREFIX=imdb
ALLOWED_HOSTS=localhost,127.0.0.1,0.0.0.0,nginx,app1,app2,app3
CSRF_TRUSTED_ORIGINS=http://localhost:8000,http://127.0.0.1:8000
```

Если запускаешь проект не в Docker, `DB_HOST` и `REDIS_URL` нужно заменить на свои адреса.

## Запуск через Docker

1. Склонируй проект и перейди в его папку.
2. Создай файл `.env`.
3. Собери и подними контейнеры:

```bash
docker compose up --build
```

4. Выполни миграции:

```bash
docker compose exec app1 python manage.py migrate
```

5. Создай суперпользователя, если нужен доступ в админку:

```bash
docker compose exec app1 python manage.py createsuperuser
```

6. Импортируй IMDb-данные в базу. Ожидается папка с TSV-файлами:

```bash
docker compose exec app1 python manage.py import_imdb --path /app/data/imdb --truncate
```

Если нужны только отдельные части данных, у команды `import_imdb` есть флаги:

- `--skip-titles`
- `--skip-ratings`
- `--skip-persons`
- `--skip-crew`
- `--skip-principals`

## Где взять данные IMDb

Импорт ожидает такие файлы в каталоге `data/imdb`:

- `title.basics.tsv`
- `title.ratings.tsv`
- `name.basics.tsv`
- `title.crew.tsv`
- `title.principals.tsv`

## Доступные сервисы после запуска

- API: `http://localhost:8000/`
- Swagger: `http://localhost:8000/api/docs/`
- Redoc: `http://localhost:8000/api/redoc/`
- Schema: `http://localhost:8000/api/schema/`
