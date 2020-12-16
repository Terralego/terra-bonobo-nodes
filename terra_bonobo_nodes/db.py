from django.contrib.postgres.fields.jsonb import KeyTextTransform
from django.db.models import FloatField


class KeyFloatTransform(KeyTextTransform):
    """Cast query as float"""

    output_field = FloatField()

    def as_sql(self, *args, **kwargs):
        query, params = super().as_sql(*args, **kwargs)
        return f"{query}::float", params
