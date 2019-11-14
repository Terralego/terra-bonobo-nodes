from decimal import Decimal

from bonobo.config import Configurable, Option
from django.db import connections


class SQLExtract(Configurable):
    sql_query = Option(str, required=True, positional=True)
    identifier = Option(str, required=True, positional=True)
    db_alias = Option(str, positional=True, default="default")

    def __call__(self, *args, **kwargs):
        with connections[self.db_alias].cursor() as cursor:
            cursor.execute(self.sql_query)
            columns = [col[0] for col in cursor.description]

            for row in cursor.fetchall():
                properties = {}
                for k, v in zip(columns, row):
                    if isinstance(v, Decimal):
                        v = float(str(v))
                    properties[k] = v
                identifier = properties[self.identifier]

                yield identifier, properties


class AttributeFromSQL(Configurable):
    sql_query = Option(str, required=True, positional=True)
    property = Option(str, required=True, positional=True)
    db_alias = Option(str, positional=True, default="default")

    def __call__(self, identifier, record, *args, **kwargs):
        with connections[self.db_alias].cursor() as cursor:
            cursor.execute(self.sql_query, [identifier, ])
            columns = [col[0] for col in cursor.description]

            attr_data = []

            for row in cursor.fetchall():
                properties = {}
                for k, v in zip(columns, row):
                    if isinstance(v, Decimal):
                        v = float(str(v))
                    properties[k] = v
                attr_data.append(properties)

            record[self.property] = attr_data

        return identifier, record
