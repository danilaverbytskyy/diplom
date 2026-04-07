from rest_framework import serializers

from main.models import Crew, Genre, Person, Principal, Rating, Title


class GenreSerializer(serializers.ModelSerializer):
    class Meta:
        model = Genre
        fields = (
            'id',
            'name',
        )


class RatingSerializer(serializers.ModelSerializer):
    average_rating = serializers.SerializerMethodField()

    class Meta:
        model = Rating
        fields = (
            'average_rating',
            'num_votes',
        )

    def get_average_rating(self, obj):
        if obj.average_rating_tenths is None:
            return None
        return obj.average_rating_tenths / 10


class TitleListSerializer(serializers.ModelSerializer):
    rating = RatingSerializer(read_only=True)
    genres = GenreSerializer(many=True, read_only=True)
    title_type_display = serializers.CharField(source='get_title_type_display', read_only=True)

    class Meta:
        model = Title
        fields = (
            'id',
            'tconst',
            'title',
            'title_type',
            'title_type_display',
            'is_adult',
            'start_year',
            'end_year',
            'runtime_minutes',
            'rating',
            'genres',
        )


class CrewShortSerializer(serializers.ModelSerializer):
    person_name = serializers.CharField(source='person.name', read_only=True)
    role_display = serializers.CharField(source='get_role_display', read_only=True)

    class Meta:
        model = Crew
        fields = (
            'person_id',
            'person_name',
            'role',
            'role_display',
        )


class PrincipalShortSerializer(serializers.ModelSerializer):
    person_name = serializers.CharField(source='person.name', read_only=True)
    category_display = serializers.CharField(source='get_category_display', read_only=True)

    class Meta:
        model = Principal
        fields = (
            'person_id',
            'person_name',
            'ordering',
            'category',
            'category_display',
            'job',
            'characters',
        )


class TitleFullSerializer(serializers.ModelSerializer):
    rating = RatingSerializer(read_only=True)
    genres = GenreSerializer(many=True, read_only=True)
    crew_members = CrewShortSerializer(many=True, read_only=True)
    principals = PrincipalShortSerializer(many=True, read_only=True)
    title_type_display = serializers.CharField(source='get_title_type_display', read_only=True)

    class Meta:
        model = Title
        fields = (
            'id',
            'tconst',
            'title',
            'title_type',
            'title_type_display',
            'is_adult',
            'start_year',
            'end_year',
            'runtime_minutes',
            'rating',
            'genres',
            'crew_members',
            'principals',
        )


class PersonBaseSerializer(serializers.ModelSerializer):
    class Meta:
        model = Person
        fields = (
            'id',
            'nconst',
            'name',
            'birth_year',
            'death_year',
            'primary_professions',
        )


class PersonTitleFromPrincipalSerializer(serializers.ModelSerializer):
    title_tconst = serializers.CharField(source='title.tconst', read_only=True)
    title_name = serializers.CharField(source='title.title', read_only=True)
    title_type = serializers.IntegerField(source='title.title_type', read_only=True)
    title_type_display = serializers.CharField(source='title.get_title_type_display', read_only=True)
    start_year = serializers.IntegerField(source='title.start_year', read_only=True)
    average_rating = serializers.SerializerMethodField()
    num_votes = serializers.SerializerMethodField()
    category_display = serializers.CharField(source='get_category_display', read_only=True)

    class Meta:
        model = Principal
        fields = (
            'title_tconst',
            'title_name',
            'title_type',
            'title_type_display',
            'start_year',
            'average_rating',
            'num_votes',
            'ordering',
            'category',
            'category_display',
            'job',
            'characters',
        )

    def get_average_rating(self, obj):
        rating = getattr(obj.title, 'rating', None)
        if rating is None or rating.average_rating_tenths is None:
            return None
        return rating.average_rating_tenths / 10

    def get_num_votes(self, obj):
        rating = getattr(obj.title, 'rating', None)
        if rating is None:
            return None
        return rating.num_votes


class PersonTitleFromCrewSerializer(serializers.ModelSerializer):
    title_tconst = serializers.CharField(source='title.tconst', read_only=True)
    title_name = serializers.CharField(source='title.title', read_only=True)
    title_type = serializers.IntegerField(source='title.title_type', read_only=True)
    title_type_display = serializers.CharField(source='title.get_title_type_display', read_only=True)
    start_year = serializers.IntegerField(source='title.start_year', read_only=True)
    average_rating = serializers.SerializerMethodField()
    num_votes = serializers.SerializerMethodField()
    role_display = serializers.CharField(source='get_role_display', read_only=True)

    class Meta:
        model = Crew
        fields = (
            'title_tconst',
            'title_name',
            'title_type',
            'title_type_display',
            'start_year',
            'average_rating',
            'num_votes',
            'role',
            'role_display',
        )

    def get_average_rating(self, obj):
        rating = getattr(obj.title, 'rating', None)
        if rating is None or rating.average_rating_tenths is None:
            return None
        return rating.average_rating_tenths / 10

    def get_num_votes(self, obj):
        rating = getattr(obj.title, 'rating', None)
        if rating is None:
            return None
        return rating.num_votes


class PersonFullSerializer(serializers.ModelSerializer):
    principal_titles = PersonTitleFromPrincipalSerializer(many=True, read_only=True)
    crew_titles = PersonTitleFromCrewSerializer(many=True, read_only=True)

    class Meta:
        model = Person
        fields = (
            'id',
            'nconst',
            'name',
            'birth_year',
            'death_year',
            'primary_professions',
            'principal_titles',
            'crew_titles',
        )
