from rest_framework import serializers
from .models import Genre, Person, Movie, Review


class GenreSerializer(serializers.ModelSerializer):
    class Meta:
        model = Genre
        fields = ["id", "name", "slug"]


class PersonSerializer(serializers.ModelSerializer):
    class Meta:
        model = Person
        fields = ["id", "first_name", "last_name", "role"]


class ReviewSerializer(serializers.ModelSerializer):
    user = serializers.StringRelatedField(read_only=True)

    class Meta:
        model = Review
        fields = ["id", "user", "text", "rating", "created_at"]
        read_only_fields = ["id", "user", "created_at"]


class MovieListSerializer(serializers.ModelSerializer):
    genres = GenreSerializer(many=True, read_only=True)
    average_rating = serializers.FloatField(read_only=True)

    class Meta:
        model = Movie
        fields = [
            "id",
            "title",
            "release_year",
            "duration",
            "genres",
            "average_rating",
        ]


class MovieDetailSerializer(serializers.ModelSerializer):
    genres = GenreSerializer(many=True, read_only=True)
    persons = PersonSerializer(many=True, read_only=True)
    reviews = ReviewSerializer(many=True, read_only=True)
    average_rating = serializers.FloatField(read_only=True)

    class Meta:
        model = Movie
        fields = [
            "id",
            "title",
            "description",
            "release_year",
            "duration",
            "genres",
            "persons",
            "reviews",
            "average_rating",
            "created_at",
        ]
        read_only_fields = ["id", "created_at", "average_rating"]