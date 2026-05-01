python manage.py import_imdb --path ./data/imdb

docker exec -it redis redis-cli -n 1 --scan --pattern "imdb:*"

docker compose down && docker compose up --build